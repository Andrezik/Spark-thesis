""" JSON -> PySpark program generator

Takes a pipeline specification (a plain dict, built by the frontend form and
sent to the backend as JSON) and produces a runnable PySpark program as source
code.

This is the piece that makes the backend "build the program that will run",
instead of the user having to upload a ready-made .jar/.py themselves.

Specification shape
-------------------
{
  "name": "my-pipeline",

  "input":  {"path": "/data/in.csv", "format": "csv",
             "options": {"header": "true", "inferSchema": "true"}},

  "steps":  [                                   # optional, applied in order
      {"type": "select", "columns": ["a", "b"]},
      {"type": "filter", "condition": "a > 10"},
      {"type": "dropna"},
      {"type": "limit",  "n": 1000},
      {"type": "string_indexer",   "inputCol": "city", "outputCol": "city_idx"},
      {"type": "one_hot_encoder",  "inputCol": "city_idx", "outputCol": "city_vec"}
  ],

  "stages": [                                   # one or more, applied in order
      {"name": "kmeans",
       "features": ["a", "b"],
       "scaler": "minmax",                      # optional: minmax | standard
       "params": {"k": 3, "maxIter": 20}},

      {"name": "logistic_regression",           # may use prediction_1 as a feature
       "features": ["a", "b", "prediction_1"],
       "label": "churned",
       "params": {"maxIter": 10}},

      {"name": "pca",                           # feature extraction: adds 'pca_3'
       "features": ["a", "b", "c"],
       "params": {"k": 2}},

      {"name": "tf_idf",                        # text input: exactly one column
       "features": ["review_text"],
       "params": {"numFeatures": 1024}}
  ],

  "output": {"path": "/data/out", "format": "parquet", "mode": "overwrite"}
}

Chaining
--------
Each stage transforms the DataFrame and adds its own columns, so a later stage
sees everything the earlier ones produced. To keep the columns apart, stage N
writes to 'features_N' and 'prediction_N' (plus 'raw_prediction_N' and
'probability_N' for classifiers). A stage can therefore list 'prediction_1'
among its own features. Feature extraction stages add one vector column
instead ('pca_N', 'selected_N', 'tfidf_N' or 'w2v_N'), which later stages can
use the same way.

Result preview
--------------
When generate_program() is called with preview=True, the program finally
prints the first rows of the resulting DataFrame as JSON, between two marker
lines. The executor extracts them from the driver's output and stores them in
the shared folder; the UI shows them when the user opens the finished job.

A single "algorithm" object is still accepted in place of "stages" and is
treated as a one-stage pipeline.

Safety note
-----------
Every user-supplied value is embedded into the generated source with repr(),
so a value can never "break out" of its literal and inject arbitrary code.
The one exception is the SQL expression of a "filter" step, which is by
definition an expression - it is still embedded as a string literal and is
evaluated by Spark's SQL parser, not by Python.
"""

''' LIBRARIES '''
import re

''' ALGORITHM CATALOGUE '''
# Each entry describes one supported algorithm:
#   estimator      -> the pyspark.ml class to instantiate
#   module         -> where to import it from
#   category       -> 'clustering' | 'classification' | 'regression' | 'feature_extraction'
#   label_required -> does it need a label column (supervised)?
#   prediction_cols-> the *Col parameters whose value must be made unique per stage
#   params         -> {param_name: (python_type, default)}
#
# Feature extraction entries additionally carry:
#   input          -> 'vector': numeric feature columns, assembled (and optionally
#                                scaled) exactly like the other algorithms
#                     'text'  : exactly one text column, tokenized into words first
#   input_param    -> the parameter of the estimator that receives the input column
#   output_col     -> base name of the column the stage adds ('pca' -> 'pca_1')
ALGORITHMS = {
    'kmeans': {
        'estimator': 'KMeans',
        'module': 'pyspark.ml.clustering',
        'category': 'clustering',
        'label_required': False,
        'prediction_cols': ['predictionCol'],
        'params': {'k': (int, 3), 'maxIter': (int, 20), 'seed': (int, 1)},
    },
    'bisecting_kmeans': {
        'estimator': 'BisectingKMeans',
        'module': 'pyspark.ml.clustering',
        'category': 'clustering',
        'label_required': False,
        'prediction_cols': ['predictionCol'],
        'params': {'k': (int, 4), 'maxIter': (int, 20), 'seed': (int, 1)},
    },
    'gaussian_mixture': {
        'estimator': 'GaussianMixture',
        'module': 'pyspark.ml.clustering',
        'category': 'clustering',
        'label_required': False,
        'prediction_cols': ['predictionCol', 'probabilityCol'],
        'params': {'k': (int, 2), 'maxIter': (int, 100), 'seed': (int, 1)},
    },
    'logistic_regression': {
        'estimator': 'LogisticRegression',
        'module': 'pyspark.ml.classification',
        'category': 'classification',
        'label_required': True,
        'prediction_cols': ['predictionCol', 'rawPredictionCol', 'probabilityCol'],
        'params': {'maxIter': (int, 10), 'regParam': (float, 0.0)},
    },
    'decision_tree': {
        'estimator': 'DecisionTreeClassifier',
        'module': 'pyspark.ml.classification',
        'category': 'classification',
        'label_required': True,
        'prediction_cols': ['predictionCol', 'rawPredictionCol', 'probabilityCol'],
        'params': {'maxDepth': (int, 5)},
    },
    'random_forest': {
        'estimator': 'RandomForestClassifier',
        'module': 'pyspark.ml.classification',
        'category': 'classification',
        'label_required': True,
        'prediction_cols': ['predictionCol', 'rawPredictionCol', 'probabilityCol'],
        'params': {'numTrees': (int, 20), 'maxDepth': (int, 5), 'seed': (int, 1)},
    },
    'linear_regression': {
        'estimator': 'LinearRegression',
        'module': 'pyspark.ml.regression',
        'category': 'regression',
        'label_required': True,
        'prediction_cols': ['predictionCol'],
        'params': {'maxIter': (int, 10), 'regParam': (float, 0.0)},
    },
    'decision_tree_regressor': {
        'estimator': 'DecisionTreeRegressor',
        'module': 'pyspark.ml.regression',
        'category': 'regression',
        'label_required': True,
        'prediction_cols': ['predictionCol'],
        'params': {'maxDepth': (int, 5)},
    },
    # --- feature extraction -------------------------------------------------
    # These do not predict anything: they add a new vector column that later
    # stages can list among their features (e.g. 'pca_1').
    'pca': {
        'estimator': 'PCA',
        'module': 'pyspark.ml.feature',
        'category': 'feature_extraction',
        'label_required': False,
        'input': 'vector',
        'input_param': 'inputCol',
        'output_col': 'pca',
        'params': {'k': (int, 2)},
    },
    'chisq_selector': {
        'estimator': 'ChiSqSelector',
        'module': 'pyspark.ml.feature',
        'category': 'feature_extraction',
        'label_required': True,
        'input': 'vector',
        'input_param': 'featuresCol',
        'output_col': 'selected',
        'params': {'numTopFeatures': (int, 2)},
    },
    'tf_idf': {
        'estimator': 'IDF',
        'module': 'pyspark.ml.feature',
        'category': 'feature_extraction',
        'label_required': False,
        'input': 'text',
        'input_param': 'inputCol',
        'output_col': 'tfidf',
        # numFeatures belongs to HashingTF, minDocFreq to IDF (see _text_stage_lines)
        'params': {'numFeatures': (int, 1024), 'minDocFreq': (int, 0)},
    },
    'word2vec': {
        'estimator': 'Word2Vec',
        'module': 'pyspark.ml.feature',
        'category': 'feature_extraction',
        'label_required': False,
        'input': 'text',
        'input_param': 'inputCol',
        'output_col': 'w2v',
        # Spark's own minCount default is 5, too high for small sample datasets
        'params': {'vectorSize': (int, 10), 'minCount': (int, 1), 'seed': (int, 1)},
    },
}

# Human readable names for the categories, in the order the form should show them
CATEGORIES = [
    ('clustering', 'Clustering'),
    ('classification', 'Classification'),
    ('regression', 'Regression'),
    ('feature_extraction', 'Feature Extraction'),
]


def is_extractor(definition):
    """ True for feature extraction entries, which add a column instead of predicting. """
    return definition['category'] == 'feature_extraction'


def is_text_input(definition):
    """ True for algorithms that read one text column instead of numeric features. """
    return definition.get('input') == 'text'

''' FEATURE SCALERS '''
# Applied to the assembled feature vector of a stage, before the estimator.
SCALERS = {
    'minmax': {'transformer': 'MinMaxScaler', 'module': 'pyspark.ml.feature'},
    'standard': {'transformer': 'StandardScaler', 'module': 'pyspark.ml.feature'},
}

''' COLUMN-LEVEL FEATURE TRANSFORMERS '''
# Applied as ordinary steps, before any feature vector is assembled.
COLUMN_TRANSFORMERS = {
    'string_indexer': {'transformer': 'StringIndexer', 'module': 'pyspark.ml.feature', 'fits': True},
    'one_hot_encoder': {'transformer': 'OneHotEncoder', 'module': 'pyspark.ml.feature', 'fits': True},
}

SUPPORTED_FORMATS = {'csv', 'json', 'parquet', 'orc', 'text'}
SUPPORTED_WRITE_MODES = {'overwrite', 'append', 'ignore', 'error'}
# Plain DataFrame operations
DATAFRAME_STEPS = {'select', 'filter', 'dropna', 'limit'}
SUPPORTED_STEPS = DATAFRAME_STEPS | set(COLUMN_TRANSFORMERS)

# A conservative identifier check for column names
_COLUMN_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_ .-]*$')


class PipelineSpecError(Exception):
    """ Raised when the pipeline specification is invalid. """

    def __init__(self, errors):
        self.errors = errors  # list of human readable strings
        super().__init__('Invalid pipeline specification: ' + '; '.join(errors))


''' HELPERS '''

def _stages_of(spec):
    """
    Returns the list of algorithm stages.

    Accepts either the current "stages" list or a single "algorithm" object,
    so specifications produced before multi-stage support still work.
    """
    if isinstance(spec, dict):
        if isinstance(spec.get('stages'), list):
            return spec['stages']
        if isinstance(spec.get('algorithm'), dict):
            return [spec['algorithm']]
    return []


def _features_col(index, scaled=False):
    return f'features_{index}_raw' if scaled else f'features_{index}'


def _prediction_col(param_name, index):
    """ 'predictionCol' -> 'prediction_1', 'rawPredictionCol' -> 'raw_prediction_1' """
    base = param_name[:-3]  # drop the trailing 'Col'
    snake = re.sub(r'(?<!^)(?=[A-Z])', '_', base).lower()
    return f'{snake}_{index}'


''' VALIDATION '''

# An absolute path ('/mnt/shared/data/x.csv') or a URI with a scheme ('s3a://...')
_URI_SCHEME_RE = re.compile(r'^[A-Za-z][A-Za-z0-9+.-]*://')


def _validate_path(path, where, errors):
    """
    Data paths must be absolute. A relative one ('mnt/shared/x.csv') is resolved
    by Spark against the working directory of the pod (/opt/spark/work-dir), so
    the job would only fail later, on the cluster, with PATH_NOT_FOUND.
    """
    if not (path.startswith('/') or _URI_SCHEME_RE.match(path)):
        errors.append(f"{where}: {path!r} is not an absolute path - did you mean '/{path}'?")


def _validate_column(name, where, errors):
    if not isinstance(name, str) or not _COLUMN_RE.match(name):
        errors.append(f'{where}: invalid column name {name!r}')


def _validate_step(step, where, errors):
    step_type = step.get('type')
    if step_type not in SUPPORTED_STEPS:
        errors.append(f'{where}.type: {step_type!r} is not one of {sorted(SUPPORTED_STEPS)}')
        return

    if step_type == 'select':
        columns = step.get('columns')
        if not isinstance(columns, list) or not columns:
            errors.append(f'{where}.columns: must be a non-empty list')
        else:
            for column in columns:
                _validate_column(column, f'{where}.columns', errors)

    elif step_type == 'filter':
        if not isinstance(step.get('condition'), str) or not step['condition'].strip():
            errors.append(f'{where}.condition: must be a non-empty string')

    elif step_type == 'limit':
        try:
            if int(step.get('n')) <= 0:
                errors.append(f'{where}.n: must be a positive integer')
        except (TypeError, ValueError):
            errors.append(f'{where}.n: must be a positive integer')

    elif step_type in COLUMN_TRANSFORMERS:
        for field in ('inputCol', 'outputCol'):
            value = step.get(field)
            if not value:
                errors.append(f'{where}.{field}: is required for {step_type}')
            else:
                _validate_column(value, f'{where}.{field}', errors)


def _validate_stage(stage, where, errors):
    if not isinstance(stage, dict):
        errors.append(f'{where}: must be an object')
        return

    definition = ALGORITHMS.get(stage.get('name'))
    if definition is None:
        errors.append(f'{where}.name: {stage.get("name")!r} is not one of {sorted(ALGORITHMS)}')
        return

    features = stage.get('features')
    if not isinstance(features, list) or not features:
        errors.append(f'{where}.features: must be a non-empty list of column names')
    else:
        for column in features:
            _validate_column(column, f'{where}.features', errors)
        if is_text_input(definition) and len(features) != 1:
            errors.append(f'{where}.features: {stage["name"]} takes exactly one text column')

    if is_text_input(definition) and stage.get('scaler'):
        errors.append(f'{where}.scaler: scaling does not apply to text input ({stage["name"]})')

    if definition['label_required']:
        label = stage.get('label')
        if not label:
            errors.append(f'{where}.label: is required for {stage["name"]}')
        else:
            _validate_column(label, f'{where}.label', errors)
            # A model given the answer among its inputs only learns to copy it
            if isinstance(features, list) and label in features:
                errors.append(f'{where}.features: must not contain the label column {label!r}')

    scaler = stage.get('scaler')
    if scaler not in (None, '', *SCALERS):
        errors.append(f'{where}.scaler: {scaler!r} is not one of {sorted(SCALERS)}')

    params = stage.get('params', {})
    if not isinstance(params, dict):
        errors.append(f'{where}.params: must be an object')
    else:
        for key, value in params.items():
            if key not in definition['params']:
                errors.append(f'{where}.params.{key}: not supported by {stage["name"]}')
                continue
            expected_type = definition['params'][key][0]
            try:
                expected_type(value)
            except (TypeError, ValueError):
                errors.append(f'{where}.params.{key}: expected {expected_type.__name__}')


def validate_spec(spec):
    """ Validates a pipeline specification. Returns a list of error strings (empty == valid). """
    errors = []

    if not isinstance(spec, dict):
        return ['specification must be a JSON object']

    # -- name -------------------------------------------------------------
    name = spec.get('name')
    if name is not None and not isinstance(name, str):
        errors.append('name: must be a string')

    # -- input ------------------------------------------------------------
    source = spec.get('input')
    if not isinstance(source, dict):
        errors.append('input: missing or not an object')
    else:
        if not source.get('path'):
            errors.append('input.path: is required')
        elif not isinstance(source['path'], str):
            errors.append('input.path: must be a string')
        else:
            _validate_path(source['path'], 'input.path', errors)
        fmt = source.get('format', 'csv')
        if fmt not in SUPPORTED_FORMATS:
            errors.append(f'input.format: {fmt!r} is not one of {sorted(SUPPORTED_FORMATS)}')
        options = source.get('options', {})
        if not isinstance(options, dict):
            errors.append('input.options: must be an object')

    # -- steps ------------------------------------------------------------
    steps = spec.get('steps', [])
    if not isinstance(steps, list):
        errors.append('steps: must be a list')
    else:
        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                errors.append(f'steps[{i}]: must be an object')
            else:
                _validate_step(step, f'steps[{i}]', errors)

    # -- stages -----------------------------------------------------------
    stages = _stages_of(spec)
    if not stages:
        errors.append('stages: at least one algorithm stage is required')
    else:
        for i, stage in enumerate(stages):
            _validate_stage(stage, f'stages[{i}]', errors)

    # -- output -----------------------------------------------------------
    sink = spec.get('output')
    if sink is not None:
        if not isinstance(sink, dict):
            errors.append('output: must be an object')
        else:
            if not sink.get('path'):
                errors.append('output.path: is required when output is given')
            elif isinstance(sink['path'], str):
                _validate_path(sink['path'], 'output.path', errors)
            fmt = sink.get('format', 'parquet')
            if fmt not in SUPPORTED_FORMATS:
                errors.append(f'output.format: {fmt!r} is not one of {sorted(SUPPORTED_FORMATS)}')
            mode = sink.get('mode', 'overwrite')
            if mode not in SUPPORTED_WRITE_MODES:
                errors.append(f'output.mode: {mode!r} is not one of {sorted(SUPPORTED_WRITE_MODES)}')

    return errors


''' GENERATION '''

def _coerce_params(algorithm_name, params):
    """ Casts the given params to the types declared in the catalogue, filling in defaults. """
    declared = ALGORITHMS[algorithm_name]['params']
    resolved = {}
    for key, (param_type, default) in declared.items():
        raw = params.get(key, default)
        resolved[key] = param_type(raw)
    return resolved


def _reader_lines(source):
    """ Lines that read the input DataFrame. """
    path = source['path']
    fmt = source.get('format', 'csv')
    options = source.get('options') or {}

    lines = ['reader = spark.read.format({!r})'.format(fmt)]
    for key, value in options.items():
        # Spark expects option values as strings; booleans become "true"/"false"
        if isinstance(value, bool):
            value = 'true' if value else 'false'
        lines.append('reader = reader.option({!r}, {!r})'.format(str(key), str(value)))
    lines.append('df = reader.load({!r})'.format(path))
    return lines


def _step_lines(steps):
    """ Lines for the optional transformation steps. """
    lines = []
    for index, step in enumerate(steps, start=1):
        step_type = step['type']

        if step_type == 'select':
            lines.append('df = df.select(*{!r})'.format(list(step['columns'])))
        elif step_type == 'filter':
            lines.append('df = df.filter({!r})'.format(step['condition']))
        elif step_type == 'dropna':
            lines.append('df = df.dropna()')
        elif step_type == 'limit':
            lines.append('df = df.limit({!r})'.format(int(step['n'])))
        elif step_type in COLUMN_TRANSFORMERS:
            definition = COLUMN_TRANSFORMERS[step_type]
            variable = f'{step_type}_{index}'
            lines.append('{} = {}(inputCol={!r}, outputCol={!r})'.format(
                variable, definition['transformer'], step['inputCol'], step['outputCol']))
            if definition['fits']:
                # StringIndexer and OneHotEncoder are Estimators: they learn from
                # the data first, then transform it.
                lines.append('df = {}.fit(df).transform(df)'.format(variable))
            else:
                lines.append('df = {}.transform(df)'.format(variable))
    return lines


def _output_col(definition, index):
    """ The column a feature extraction stage adds, e.g. 'pca_1'. """
    return f"{definition['output_col']}_{index}"


def _words_col(index):
    return f'words_{index}'


def _tf_col(index):
    return f'tf_{index}'


def output_column(stage, index):
    """
    The main column a stage adds - the one a later stage would use as a feature:
    'prediction_N' for the predicting algorithms, e.g. 'pca_N' for feature extraction.
    """
    definition = ALGORITHMS[stage['name']]
    if is_extractor(definition):
        return _output_col(definition, index)
    return _prediction_col('predictionCol', index)


def _vector_input_lines(stage, index):
    """
    Lines that gather a stage's numeric feature columns into one vector column
    (VectorAssembler) and, if requested, scale it. Returns (lines, vector_column).
    """
    features = list(stage['features'])
    scaler_name = stage.get('scaler') or None

    features_col = _features_col(index)
    assembler_output = _features_col(index, scaled=bool(scaler_name))

    lines = [
        'assembler_{} = VectorAssembler(inputCols={!r}, outputCol={!r})'.format(
            index, features, assembler_output),
        'data = assembler_{}.transform(data)'.format(index),
    ]

    if scaler_name:
        scaler = SCALERS[scaler_name]
        lines.append('scaler_{} = {}(inputCol={!r}, outputCol={!r})'.format(
            index, scaler['transformer'], assembler_output, features_col))
        lines.append('data = scaler_{}.fit(data).transform(data)'.format(index))

    return lines, features_col


def _text_stage_lines(stage, index, definition, params):
    """
    Lines for the text based feature extraction stages (TF-IDF, Word2Vec).

    Both start by splitting the text column into words (Tokenizer). TF-IDF then
    counts the words of every row (HashingTF) and re-weights the counts by how
    rare each word is across all rows (IDF); Word2Vec learns a vector per word
    and represents each row by the average vector of its words.
    """
    text_column = stage['features'][0]
    words_col = _words_col(index)
    output_col = _output_col(definition, index)

    lines = [
        'tokenizer_{} = Tokenizer(inputCol={!r}, outputCol={!r})'.format(index, text_column, words_col),
        'data = tokenizer_{}.transform(data)'.format(index),
    ]

    if stage['name'] == 'tf_idf':
        tf_col = _tf_col(index)
        lines.append('hashing_tf_{} = HashingTF(inputCol={!r}, outputCol={!r}, numFeatures={!r})'.format(
            index, words_col, tf_col, params['numFeatures']))
        lines.append('data = hashing_tf_{}.transform(data)'.format(index))
        lines.append('estimator_{} = IDF(inputCol={!r}, outputCol={!r}, minDocFreq={!r})'.format(
            index, tf_col, output_col, params['minDocFreq']))
    else:  # word2vec
        args = ['inputCol={!r}'.format(words_col), 'outputCol={!r}'.format(output_col)]
        args += ['{}={!r}'.format(key, value) for key, value in params.items()]
        lines.append('estimator_{} = {}({})'.format(index, definition['estimator'], ', '.join(args)))

    return lines


def _stage_lines(stage, index):
    """
    Lines for one algorithm stage.

    Every stage reads the DataFrame produced by the previous one and adds its
    own columns, suffixed with the stage number so nothing collides.
    """
    name = stage['name']
    definition = ALGORITHMS[name]
    params = _coerce_params(name, stage.get('params') or {})

    lines = [f'# --- stage {index}: {name} ---']

    if is_text_input(definition):
        lines += _text_stage_lines(stage, index, definition, params)
    else:
        input_lines, features_col = _vector_input_lines(stage, index)
        lines += input_lines

        if is_extractor(definition):
            # PCA / ChiSqSelector: vector in, new (smaller) vector out
            estimator_args = [
                '{}={!r}'.format(definition['input_param'], features_col),
                'outputCol={!r}'.format(_output_col(definition, index)),
            ]
        else:
            estimator_args = ['featuresCol={!r}'.format(features_col)]
            for prediction_param in definition['prediction_cols']:
                estimator_args.append('{}={!r}'.format(prediction_param, _prediction_col(prediction_param, index)))

        if definition['label_required']:
            estimator_args.append('labelCol={!r}'.format(stage['label']))
        for key, value in params.items():
            estimator_args.append('{}={!r}'.format(key, value))

        lines.append('estimator_{} = {}({})'.format(index, definition['estimator'], ', '.join(estimator_args)))

    lines.append('model_{} = estimator_{}.fit(data)'.format(index, index))
    lines.append('data = model_{}.transform(data)'.format(index))
    return lines


def _helper_columns(stages):
    """
    Columns that only exist to feed an algorithm (assembled/scaled feature
    vectors, word lists, raw predictions and probabilities). They are dropped
    before the result is written, since vector and array columns cannot be
    written to every format.
    """
    columns = []
    for index, stage in enumerate(stages, start=1):
        definition = ALGORITHMS[stage['name']]
        if is_text_input(definition):
            columns.append(_words_col(index))
            if stage['name'] == 'tf_idf':
                columns.append(_tf_col(index))
            continue
        columns.append(_features_col(index))
        if stage.get('scaler'):
            columns.append(_features_col(index, scaled=True))
        for prediction_param in definition.get('prediction_cols', []):
            if prediction_param != 'predictionCol':
                columns.append(_prediction_col(prediction_param, index))
    return columns


def _extracted_columns(stages):
    """ The vector columns produced by feature extraction stages - these ARE the result. """
    return [_output_col(ALGORITHMS[stage['name']], index)
            for index, stage in enumerate(stages, start=1)
            if is_extractor(ALGORITHMS[stage['name']])]


def _writer_lines(sink, stages):
    """ Lines that write the result out, or just show it when no output is configured. """
    if not sink:
        return ['result = data', 'result.show(20, truncate=False)']

    fmt = sink.get('format', 'parquet')
    mode = sink.get('mode', 'overwrite')
    lines = [
        '# Helper vector columns cannot be written to every format, so they are dropped',
        'result = data.drop(*{!r})'.format(_helper_columns(stages)),
    ]

    # Extracted features are kept, as plain arrays of numbers (CSV cannot hold
    # arrays either, so there they are written as text: "[0.1, 0.2]")
    for column in _extracted_columns(stages):
        converted = 'vector_to_list(result[{!r}])'.format(column)
        if fmt == 'csv':
            converted += ".cast('string')"
        lines.append('result = result.withColumn({!r}, {})'.format(column, converted))

    lines.append('result.write.format({!r}).mode({!r}).save({!r})'.format(fmt, mode, sink['path']))
    return lines


def _needs_vector_helper(spec, stages):
    """ True when extracted feature vectors have to be written out (as arrays). """
    return bool(spec.get('output')) and bool(_extracted_columns(stages))


def _imports(spec, stages):
    """ The import lines the generated program needs, without duplicates. """
    needed = set()

    for stage in stages:
        definition = ALGORITHMS[stage['name']]
        needed.add('from {} import {}'.format(definition['module'], definition['estimator']))
        if is_text_input(definition):
            needed.add('from pyspark.ml.feature import Tokenizer')
            if stage['name'] == 'tf_idf':
                needed.add('from pyspark.ml.feature import HashingTF')
        else:
            needed.add('from pyspark.ml.feature import VectorAssembler')
        scaler_name = stage.get('scaler')
        if scaler_name:
            scaler = SCALERS[scaler_name]
            needed.add('from {} import {}'.format(scaler['module'], scaler['transformer']))

    for step in spec.get('steps') or []:
        definition = COLUMN_TRANSFORMERS.get(step.get('type'))
        if definition:
            needed.add('from {} import {}'.format(definition['module'], definition['transformer']))

    if _needs_vector_helper(spec, stages):
        needed.add('from pyspark.sql.functions import udf')
        needed.add('from pyspark.sql.types import ArrayType, DoubleType')

    return sorted(needed)


def generate_program(spec, skip_validation=False, preview=False):
    """
    Builds a runnable PySpark program from a pipeline specification.

    If preview is True, the program finally prints a preview of the resulting
    DataFrame (the first PREVIEW_ROWS rows, as one line of JSON between the
    PREVIEW_BEGIN / PREVIEW_END markers). The executor finds it in the driver's
    output and stores it in the shared folder, and the UI shows it when the user
    opens the results of the finished job.

    Returns the program source code as a string.
    Raises PipelineSpecError if the specification is invalid.
    """
    if not skip_validation:
        errors = validate_spec(spec)
        if errors:
            raise PipelineSpecError(errors)

    stages = _stages_of(spec)
    app_name = spec.get('name') or 'generated-pipeline'

    body = []
    body += _reader_lines(spec['input'])
    body.append('')

    steps = spec.get('steps') or []
    if steps:
        body += _step_lines(steps)
        body.append('')

    body.append('data = df')
    body.append('')

    for index, stage in enumerate(stages, start=1):
        body += _stage_lines(stage, index)
        body.append('')

    body += _writer_lines(spec.get('output'), stages)

    imports = _imports(spec, stages)
    helpers = VECTOR_HELPER if _needs_vector_helper(spec, stages) else ''
    if preview:
        body.append('')
        body.append('# Preview of the result, shown by the "Results" button of the UI')
        body.append('print_preview(data)')
        imports = ['import json', ''] + imports
        helpers += PREVIEW_HELPERS

    indented_body = '\n'.join('    ' + line if line else '' for line in body)
    stage_names = ' -> '.join(stage['name'] for stage in stages)

    return PROGRAM_TEMPLATE.format(
        app_name=app_name,
        stage_names=stage_names,
        imports='\n'.join(imports),
        helpers=helpers,
        body=indented_body,
    )


# Inserted when extracted feature vectors are written out. pyspark.ml.functions.
# vector_to_array would do the same, but importing that module needs pandas,
# which the Spark images do not have, so a small UDF is used instead.
VECTOR_HELPER = '''

# ML vector -> plain array of numbers, which every output format can store
vector_to_list = udf(lambda vector: vector.toArray().tolist() if vector is not None else None,
                     ArrayType(DoubleType()))
'''

# Number of result rows kept for the UI preview
PREVIEW_ROWS = 50

# Lines that enclose the preview in the driver's output. The same strings are
# used by executor/JobExecutor.py, which extracts what is between them.
#
# Why the preview is printed instead of written to a file: the driver pod could
# not write to the shared Windows folder (PermissionError), not even with the
# image's user set to root - most likely because the image's entrypoint
# switches to the unprivileged user 'spark' anyway. The executor container can
# write there, so it extracts the preview from the output and writes the file.
PREVIEW_BEGIN = '=====SPARK-GUI-PREVIEW-BEGIN====='
PREVIEW_END = '=====SPARK-GUI-PREVIEW-END====='

# Inserted into the generated program when a preview is requested. Kept as a
# separate constant (not part of PROGRAM_TEMPLATE) so its braces are not
# interpreted by str.format.
PREVIEW_HELPERS = '''

PREVIEW_ROWS = %d
PREVIEW_BEGIN = %r
PREVIEW_END = %r


def _jsonable(value):
    """ Turns a DataFrame value into something json.dump can write. """
    if hasattr(value, 'indices') and value.size > 20:
        # Long sparse vectors (e.g. TF-IDF over 1024 buckets): only the non-zero entries
        return {'size': int(value.size),
                'nonzero': {str(int(i)): round(float(v), 6) for i, v in zip(value.indices, value.values)}}
    if hasattr(value, 'toArray'):          # ML vectors (dense or sparse)
        return [round(float(x), 6) for x in value.toArray().tolist()]
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, float):
        return round(value, 6)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    return str(value)


def print_preview(data):
    """ Prints the first rows of the final DataFrame as one line of JSON, for the UI. """
    columns = data.columns
    rows = data.limit(PREVIEW_ROWS).collect()
    preview = {
        'columns': columns,
        'rows': [[_jsonable(row[i]) for i in range(len(columns))] for row in rows],
        'total_rows': data.count(),
        'preview_rows': PREVIEW_ROWS,
    }
    print(PREVIEW_BEGIN)
    print(json.dumps(preview))
    print(PREVIEW_END, flush=True)
''' % (PREVIEW_ROWS, PREVIEW_BEGIN, PREVIEW_END)


PROGRAM_TEMPLATE = '''""" Auto-generated PySpark program

Generated by the spark-submit GUI backend from a pipeline specification.
Pipeline: {app_name}
Stages: {stage_names}

Do not edit by hand - regenerate it from the UI instead.
"""

from pyspark.sql import SparkSession
{imports}
{helpers}

def main():
    spark = SparkSession.builder.appName({app_name!r}).getOrCreate()

{body}

    spark.stop()


if __name__ == '__main__':
    main()
'''


''' GLOBAL '''
print(f'\n{__file__} is running...\n')
