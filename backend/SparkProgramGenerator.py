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
       "params": {"maxIter": 10}}
  ],

  "output": {"path": "/data/out", "format": "parquet", "mode": "overwrite"}
}

Chaining
--------
Each stage transforms the DataFrame and adds its own columns, so a later stage
sees everything the earlier ones produced. To keep the columns apart, stage N
writes to 'features_N' and 'prediction_N' (plus 'raw_prediction_N' and
'probability_N' for classifiers). A stage can therefore list 'prediction_1'
among its own features.

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
#   category       -> 'clustering' | 'classification' | 'regression'
#   label_required -> does it need a label column (supervised)?
#   prediction_cols-> the *Col parameters whose value must be made unique per stage
#   params         -> {param_name: (python_type, default)}
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
}

# Human readable names for the categories, in the order the form should show them
CATEGORIES = [
    ('clustering', 'Clustering'),
    ('classification', 'Classification'),
    ('regression', 'Regression'),
]

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

    if definition['label_required']:
        label = stage.get('label')
        if not label:
            errors.append(f'{where}.label: is required for {stage["name"]}')
        else:
            _validate_column(label, f'{where}.label', errors)

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


def _stage_lines(stage, index):
    """
    Lines for one algorithm stage.

    Every stage reads the DataFrame produced by the previous one and adds its
    own columns, suffixed with the stage number so nothing collides.
    """
    name = stage['name']
    definition = ALGORITHMS[name]
    features = list(stage['features'])
    scaler_name = stage.get('scaler') or None
    params = _coerce_params(name, stage.get('params') or {})

    features_col = _features_col(index)
    assembler_output = _features_col(index, scaled=bool(scaler_name))

    lines = [
        f'# --- stage {index}: {name} ---',
        'assembler_{} = VectorAssembler(inputCols={!r}, outputCol={!r})'.format(
            index, features, assembler_output),
        'data = assembler_{}.transform(data)'.format(index),
    ]

    if scaler_name:
        scaler = SCALERS[scaler_name]
        lines.append('scaler_{} = {}(inputCol={!r}, outputCol={!r})'.format(
            index, scaler['transformer'], assembler_output, features_col))
        lines.append('data = scaler_{}.fit(data).transform(data)'.format(index))

    estimator_args = ['featuresCol={!r}'.format(features_col)]
    if definition['label_required']:
        estimator_args.append('labelCol={!r}'.format(stage['label']))
    for prediction_param in definition['prediction_cols']:
        estimator_args.append('{}={!r}'.format(prediction_param, _prediction_col(prediction_param, index)))
    for key, value in params.items():
        estimator_args.append('{}={!r}'.format(key, value))

    lines.append('estimator_{} = {}({})'.format(index, definition['estimator'], ', '.join(estimator_args)))
    lines.append('model_{} = estimator_{}.fit(data)'.format(index, index))
    lines.append('data = model_{}.transform(data)'.format(index))
    return lines


def _writer_lines(sink, stages):
    """ Lines that write the result out, or just show it when no output is configured. """
    # Vector columns cannot be written to every format, so they are dropped
    vector_columns = []
    for index, stage in enumerate(stages, start=1):
        vector_columns.append(_features_col(index))
        if stage.get('scaler'):
            vector_columns.append(_features_col(index, scaled=True))
        for prediction_param in ALGORITHMS[stage['name']]['prediction_cols']:
            if prediction_param != 'predictionCol':
                vector_columns.append(_prediction_col(prediction_param, index))

    if not sink:
        return ['result = data', 'result.show(20, truncate=False)']

    fmt = sink.get('format', 'parquet')
    mode = sink.get('mode', 'overwrite')
    return [
        '# Vector columns cannot be written to every format',
        'result = data.drop(*{!r})'.format(vector_columns),
        'result.write.format({!r}).mode({!r}).save({!r})'.format(fmt, mode, sink['path']),
    ]


def _imports(spec, stages):
    """ The import lines the generated program needs, without duplicates. """
    needed = {'from pyspark.ml.feature import VectorAssembler'}

    for stage in stages:
        definition = ALGORITHMS[stage['name']]
        needed.add('from {} import {}'.format(definition['module'], definition['estimator']))
        scaler_name = stage.get('scaler')
        if scaler_name:
            scaler = SCALERS[scaler_name]
            needed.add('from {} import {}'.format(scaler['module'], scaler['transformer']))

    for step in spec.get('steps') or []:
        definition = COLUMN_TRANSFORMERS.get(step.get('type'))
        if definition:
            needed.add('from {} import {}'.format(definition['module'], definition['transformer']))

    return sorted(needed)


def generate_program(spec, skip_validation=False):
    """
    Builds a runnable PySpark program from a pipeline specification.

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

    indented_body = '\n'.join('    ' + line if line else '' for line in body)
    stage_names = ' -> '.join(stage['name'] for stage in stages)

    return PROGRAM_TEMPLATE.format(
        app_name=app_name,
        stage_names=stage_names,
        imports='\n'.join(_imports(spec, stages)),
        body=indented_body,
    )


PROGRAM_TEMPLATE = '''""" Auto-generated PySpark program

Generated by the spark-submit GUI backend from a pipeline specification.
Pipeline: {app_name}
Stages: {stage_names}

Do not edit by hand - regenerate it from the UI instead.
"""

from pyspark.sql import SparkSession
{imports}


def main():
    spark = SparkSession.builder.appName({app_name!r}).getOrCreate()

{body}

    spark.stop()


if __name__ == '__main__':
    main()
'''


''' GLOBAL '''
print(f'\n{__file__} is running...\n')
