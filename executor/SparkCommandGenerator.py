""" JSON -> Spark-submit command generator

Turns a job's 'options' dict (same shape used across the app: spark_path,
master, deploy-mode, name, class, conf{}, additional_options{}, executable)
into a spark-submit command.

Design goals (replacing the old ad-hoc string built in JobExecutor.py):
  - Validate options against the option definitions stored in the DB
    (same source of truth used by backend/ManageOptions.py) before building
    anything.
  - Return the command as a LIST of args, so callers can do
    subprocess.run(args, shell=False) instead of concatenating a shell
    string with literal line-continuations and shell=True.
"""

''' LIBRARIES '''
import DatabaseConnection as DB

''' CONSTANTS '''
# Options dict keys that are not simple '--flag value' pairs
_SPECIAL_KEYS = {'spark_path', 'conf', 'additional_options', 'executable'}
# Order matters for readability of the generated command; any other simple
# flag present in the options dict but not listed here is still emitted,
# just after these.
_PREFERRED_FLAG_ORDER = ['master', 'deploy-mode', 'name', 'class']


class SparkCommandValidationError(Exception):
    """Raised when a job's options fail validation against the option definitions."""

    def __init__(self, errors):
        self.errors = errors  # list of {'option', 'value', 'reason'}
        super().__init__(f"Invalid job options: {errors}")


''' VALIDATION HELPERS '''

def _type_matches(value, expected_type):
    if expected_type == 'int':
        # Accept real ints, and numeric strings/values coming from JSON (e.g. memory in MB)
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            return True
        try:
            int(value)
            return True
        except (TypeError, ValueError):
            return False
    if expected_type == 'str':
        return isinstance(value, str)
    # Unknown declared type in the schema: don't block on it
    return True


def _in_range(value, expected_type, value_range):
    if not value_range:
        return True
    if expected_type == 'int':
        low, high = value_range[0], value_range[1]
        if low == '' or low is None:
            return True
        return int(low) <= int(value) <= int(high)
    # str: value_range is either [''] (unrestricted) or an explicit enum,
    # e.g. deploy-mode -> ['cluster', 'client']
    if len(value_range) == 1 and value_range[0] == '':
        return True
    return value in value_range


def _flatten_job_options(options):
    """ Maps job options onto the flat {option_name: value} shape the DB schema is keyed by. """
    flat = {}
    for key, value in options.items():
        if key == 'conf':
            for conf_key, conf_value in (value or {}).items():
                flat[conf_key] = conf_value
        elif key == 'additional_options':
            continue  # user-defined confs, not part of the fixed schema
        else:
            flat[key] = value
    return flat


def validate_options(options):
    """
    Validates a job's options against the option definitions in the database.
    Returns a list of error dicts; an empty list means the options are valid.
    """
    errors = []
    definitions_by_name = {o['name']: o for o in DB.retrieve_options()}
    flat = _flatten_job_options(options)

    # Required options must be present
    for name, definition in definitions_by_name.items():
        if definition.get('required') and name not in flat:
            errors.append({'option': name, 'value': None, 'reason': 'missing required option'})

    # Type & range checks for options that are part of the schema
    for name, value in flat.items():
        definition = definitions_by_name.get(name)
        if definition is None:
            continue  # not a recognized option; nothing to validate it against
        if value is None:
            errors.append({'option': name, 'value': value, 'reason': 'value is null'})
            continue

        expected_type = definition['values']['type']
        if not _type_matches(value, expected_type):
            errors.append({'option': name, 'value': value, 'reason': f'expected type "{expected_type}"'})
            continue

        value_range = definition['values'].get('range', [[]])[0]
        if not _in_range(value, expected_type, value_range):
            errors.append({'option': name, 'value': value, 'reason': f'out of allowed range {value_range}'})

    return errors


''' GENERATION '''

def _executable_arg(executable):
    # Historical convention in this codebase: a local path like "./job.jar"
    # becomes "local:///job.jar" for spark-submit running in k8s.
    if executable.startswith('./'):
        return f'local:///{executable[2:]}'
    return executable


def generate_spark_submit_args(options, skip_validation=False):
    """
    Builds the spark-submit command as a list of args, suitable for
    subprocess.run(args, shell=False).

    Raises SparkCommandValidationError if the options are invalid (unless
    skip_validation=True, e.g. when the caller already validated upstream).
    """
    if not skip_validation:
        errors = validate_options(options)
        if errors:
            raise SparkCommandValidationError(errors)

    args = [options['spark_path']]

    # Simple '--flag value' options, in a stable, readable order
    emitted = set(_SPECIAL_KEYS) | {'spark_path'}
    ordered_flags = _PREFERRED_FLAG_ORDER + [k for k in options if k not in _PREFERRED_FLAG_ORDER and k not in _SPECIAL_KEYS]
    for flag in ordered_flags:
        if flag in emitted or flag not in options:
            continue
        emitted.add(flag)
        value = options[flag]
        if value in (None, ''):
            continue
        if flag == 'name':
            value = str(value).replace(' ', '')
        args += [f'--{flag}', str(value)]

    # --conf key=value pairs
    for conf_key, conf_value in (options.get('conf') or {}).items():
        args += ['--conf', f'{conf_key}={conf_value}']

    # User-added --conf key=value pairs (not part of the fixed schema)
    for entry in (options.get('additional_options') or {}).values():
        args += ['--conf', f"{entry['confKey']}={entry['confValue']}"]

    args.append(_executable_arg(options['executable']))

    return args


def to_display_string(args):
    """ Human-readable, multi-line rendering of the args list (logs/UI only - never pass this to shell=True). """
    if not args:
        return ''
    lines = [args[0]]
    i = 1
    while i < len(args):
        if args[i].startswith('--') and i + 1 < len(args):
            lines.append(f'{args[i]} {args[i + 1]}')
            i += 2
        else:
            lines.append(args[i])
            i += 1
    return ' \\\n  '.join(lines)
