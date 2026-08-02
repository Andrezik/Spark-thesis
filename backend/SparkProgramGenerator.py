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
      {"type": "limit",  "n": 1000}
  ],
  "algorithm": {"name": "kmeans",
                "features": ["a", "b"],
                "params": {"k": 3, "maxIter": 20}},
  "output": {"path": "/data/out", "format": "parquet", "mode": "overwrite"}
}

Only "input" and "algorithm" are strictly required; everything else has
sensible defaults.

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
#   kind           -> 'clustering' | 'classification' | 'regression'
#   label_required -> does it need a label column (supervised)?
#   params         -> {param_name: (python_type, default)}
ALGORITHMS = {
    'kmeans': {
        'estimator': 'KMeans',
        'module': 'pyspark.ml.clustering',
        'kind': 'clustering',
        'label_required': False,
        'params': {'k': (int, 3), 'maxIter': (int, 20), 'seed': (int, 1)},
    },
    'logistic_regression': {
        'estimator': 'LogisticRegression',
        'module': 'pyspark.ml.classification',
        'kind': 'classification',
        'label_required': True,
        'params': {'maxIter': (int, 10), 'regParam': (float, 0.0)},
    },
    'linear_regression': {
        'estimator': 'LinearRegression',
        'module': 'pyspark.ml.regression',
        'kind': 'regression',
        'label_required': True,
        'params': {'maxIter': (int, 10), 'regParam': (float, 0.0)},
    },
    'decision_tree': {
        'estimator': 'DecisionTreeClassifier',
        'module': 'pyspark.ml.classification',
        'kind': 'classification',
        'label_required': True,
        'params': {'maxDepth': (int, 5)},
    },
}

SUPPORTED_FORMATS = {'csv', 'json', 'parquet', 'orc', 'text'}
SUPPORTED_WRITE_MODES = {'overwrite', 'append', 'ignore', 'error'}
SUPPORTED_STEPS = {'select', 'filter', 'dropna', 'limit'}

# A conservative identifier check for column names
_COLUMN_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_ .-]*$')


class PipelineSpecError(Exception):
    """ Raised when the pipeline specification is invalid. """

    def __init__(self, errors):
        self.errors = errors  # list of human readable strings
        super().__init__('Invalid pipeline specification: ' + '; '.join(errors))


''' VALIDATION '''

def _validate_column(name, where, errors):
    if not isinstance(name, str) or not _COLUMN_RE.match(name):
        errors.append(f'{where}: invalid column name {name!r}')


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
            where = f'steps[{i}]'
            if not isinstance(step, dict):
                errors.append(f'{where}: must be an object')
                continue
            step_type = step.get('type')
            if step_type not in SUPPORTED_STEPS:
                errors.append(f'{where}.type: {step_type!r} is not one of {sorted(SUPPORTED_STEPS)}')
                continue
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

    # -- algorithm --------------------------------------------------------
    algorithm = spec.get('algorithm')
    if not isinstance(algorithm, dict):
        errors.append('algorithm: missing or not an object')
    else:
        algorithm_name = algorithm.get('name')
        definition = ALGORITHMS.get(algorithm_name)
        if definition is None:
            errors.append(f'algorithm.name: {algorithm_name!r} is not one of {sorted(ALGORITHMS)}')
        else:
            features = algorithm.get('features')
            if not isinstance(features, list) or not features:
                errors.append('algorithm.features: must be a non-empty list of column names')
            else:
                for column in features:
                    _validate_column(column, 'algorithm.features', errors)

            if definition['label_required']:
                label = algorithm.get('label')
                if not label:
                    errors.append(f'algorithm.label: is required for {algorithm_name}')
                else:
                    _validate_column(label, 'algorithm.label', errors)

            params = algorithm.get('params', {})
            if not isinstance(params, dict):
                errors.append('algorithm.params: must be an object')
            else:
                for key, value in params.items():
                    if key not in definition['params']:
                        errors.append(f'algorithm.params.{key}: not supported by {algorithm_name}')
                        continue
                    expected_type = definition['params'][key][0]
                    try:
                        expected_type(value)
                    except (TypeError, ValueError):
                        errors.append(f'algorithm.params.{key}: expected {expected_type.__name__}')

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
    for step in steps:
        step_type = step['type']
        if step_type == 'select':
            lines.append('df = df.select(*{!r})'.format(list(step['columns'])))
        elif step_type == 'filter':
            lines.append('df = df.filter({!r})'.format(step['condition']))
        elif step_type == 'dropna':
            lines.append('df = df.dropna()')
        elif step_type == 'limit':
            lines.append('df = df.limit({!r})'.format(int(step['n'])))
    return lines


def _algorithm_lines(algorithm):
    """ Lines that assemble the features, fit the estimator and produce predictions. """
    name = algorithm['name']
    definition = ALGORITHMS[name]
    features = list(algorithm['features'])
    params = _coerce_params(name, algorithm.get('params') or {})

    lines = [
        'assembler = VectorAssembler(inputCols={!r}, outputCol="features")'.format(features),
        'data = assembler.transform(df)',
        '',
    ]

    estimator_args = ['featuresCol="features"']
    if definition['label_required']:
        estimator_args.append('labelCol={!r}'.format(algorithm['label']))
    for key, value in params.items():
        estimator_args.append('{}={!r}'.format(key, value))

    lines.append('estimator = {}({})'.format(definition['estimator'], ', '.join(estimator_args)))
    lines.append('model = estimator.fit(data)')
    lines.append('result = model.transform(data)')
    return lines


def _writer_lines(sink):
    """ Lines that write the result out, or just show it when no output is configured. """
    if not sink:
        return ['result.show(20, truncate=False)']

    fmt = sink.get('format', 'parquet')
    mode = sink.get('mode', 'overwrite')
    return [
        '# The "features" column is a Vector and cannot be written to every format',
        'output_df = result.drop("features")',
        'output_df.write.format({!r}).mode({!r}).save({!r})'.format(fmt, mode, sink['path']),
    ]


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

    algorithm = spec['algorithm']
    definition = ALGORITHMS[algorithm['name']]
    app_name = spec.get('name') or 'generated-pipeline'

    body = []
    body += _reader_lines(spec['input'])
    body.append('')

    steps = spec.get('steps') or []
    if steps:
        body += _step_lines(steps)
        body.append('')

    body += _algorithm_lines(algorithm)
    body.append('')
    body += _writer_lines(spec.get('output'))

    indented_body = '\n'.join('    ' + line if line else '' for line in body)

    return PROGRAM_TEMPLATE.format(
        app_name=app_name,
        algorithm_name=algorithm['name'],
        estimator_import='from {} import {}'.format(definition['module'], definition['estimator']),
        body=indented_body,
    )


PROGRAM_TEMPLATE = '''""" Auto-generated PySpark program

Generated by the spark-submit GUI backend from a pipeline specification.
Pipeline: {app_name}
Algorithm: {algorithm_name}

Do not edit by hand - regenerate it from the UI instead.
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
{estimator_import}


def main():
    spark = SparkSession.builder.appName({app_name!r}).getOrCreate()

{body}

    spark.stop()


if __name__ == '__main__':
    main()
'''


''' GLOBAL '''
print(f'\n{__file__} is running...\n')
