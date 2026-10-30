""" Main Backend Python Script """

''' LIBRARIES '''
from flask import Flask, request, url_for  # Flask
# from flask import redirect, url_for, render_template, make_response
import DatabaseConnection as dc     # Python file managing database
import ManageOptions as mo  # Python file checking jobs
import SparkProgramGenerator as spg  # Python file generating PySpark programs from a JSON spec
import json     # JSON files
import datetime     # Datetime
from functools import wraps     # Wrapper
import requests  # Requests
from flask_cors import CORS
import os
from werkzeug.utils import secure_filename
import uuid
import re

# Global Variables
proxmox = os.environ['PROXMOX']  # Dockerfile Configuration
ALLOWED_EXTENSIONS = {'jar', 'py', 'json'}   # txt must be removed
# Instance of flask application
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = './backend/uploads/'  # Dockerfile Configuration
CORS(app)

# Shared folder (hostPath) for generated pipelines
# ------------------------------------------------
# The same host folder is mounted at SHARED_DIR in the api and executor
# containers (docker-compose bind mount) and in every driver/executor pod
# (Kubernetes hostPath volume). Generated programs, input data and result
# previews live there, so a path is valid everywhere without copying files.
SHARED_DIR = os.environ.get('SHARED_DIR', '/mnt/shared')
# The same folder as the Kubernetes node sees it. With Docker Desktop on
# Windows, C:\Users\<user>\spark-shared is /run/desktop/mnt/host/c/Users/<user>/spark-shared
SHARED_NODE_DIR = os.environ.get('SHARED_NODE_DIR', '/run/desktop/mnt/host/c/Users/andre/spark-shared')
# Kubernetes API server as reached from the executor container
K8S_MASTER = os.environ.get('K8S_MASTER', 'k8s://https://kubernetes.docker.internal:6443')
# Node the driver pod is pinned to (the single node of Docker Desktop's cluster)
K8S_NODE_HOSTNAME = os.environ.get('K8S_NODE_HOSTNAME', 'docker-desktop')
# Image of the driver/executor pods: apache/spark plus numpy, which pyspark.ml needs
SPARK_IMAGE = os.environ.get('SPARK_IMAGE', 'spark-ml:3.5.0')

''' FUNCTIONS '''
# Check file extensions
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Wrapper function to authenticate user for each request
def authentication_required(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        # URL for testing user's request authentication
        url = proxmox + "/version"
        print(f'\nURL --> {url}\n')
        # Cookie at headers will be replaced with the ticket from frontend (UI)
        payload = {}
        # headers = {'Cookie': 'PVEAuthCookie=PVE:tsolkas@pve:657C4F34::ebGgLJOIregIHQut+FUM0VLfpjDRAOr6j51t3g6duRm+GQaSDRVKdT56UgUJ7cieHDVvq2Ou3nPXaswb/NC+PMbl1r/qmstzvQGyi5nkMGv45WRdl78+OwEcSqYnAw5JqyrWvguBBiNSXvfYSI+z182EAxOJPyivFgbgqEMXXTtyw5kgN/ANSjfQoErS9TE7m4dzdkBMEIdV0uGhBQJdy0eYVkoBQYNUNaTwhfEdMudOpNktLpwdC21zMK+7DJwyIi45jlRR1jDhW4h8BH9bPNyc7o1riSbjCuZ81gagDy2PMhi3pXC4CSDXJPs/T1MF7y8FRZfkpZ4A+zGcYAhhWA=='}
        headers = request.headers
        cookie = 'PVEAuthCookie=' + request.headers.get('Ticket')
        print(f'TYPE: {type(cookie)}\n')
        # headers.add_header('Cookie', cookie)
        print(f'Headers: {headers}')
        response = requests.request("GET", url, headers={'Cookie': cookie}, data=payload, verify=False)
        if response.status_code == 401:
            return json.dumps({'status': 'error', 'message': 'Authentication  Error'}), 401
        else:
            return f(*args, **kwargs)

    return wrap


# Main Route
@app.route('/')
def index():
    return 'Kubernetes API'


# Route for Spark Jobs
@app.route('/spark-job', methods=['GET', 'POST'], defaults={'job_id': None})
@app.route('/spark-job/<job_id>', methods=['GET', 'DELETE', 'PUT'])
@authentication_required
def spark_job(job_id):
    print('Spark job is running!\n')
    print(f'HEADERS: {request.headers}\n')
    if request.method == 'GET':
        if job_id is None:
            return json.dumps(dc.retrieve_all_jobs())
        else:
            result = dc.retrieve_job(job_id)
            if result is None:
                return json.dumps(result), 404
            else:
                return json.dumps(result)
    elif request.method == 'POST':
        if job_id is None:
            print(request.json)
            timestamp = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
            username = request.json['username']
            status = request.json['status']
            priority = request.json['priority']
            options = {
                'spark_path': request.json['options']['sparkPath'],
                'master': request.json['options']['master'],
                'deploy-mode': request.json['options']['deployMode'],
                'name': request.json['options']['name'],
                'class': request.json['options']['class'],
                'conf': {
                    'spark.executor.instances': request.json['options']['conf']['spark.executor.instances'],
                    'spark.executor.cores': request.json['options']['conf']['spark.executor.cores'],
                    'spark.executor.memory': int(request.json['options']['conf']['spark.executor.memory']),
                    'spark.kubernetes.container.image': request.json['options']['conf']['spark.kubernetes.container.image'],
                    'spark.kubernetes.driver.node.selector.kubernetes.io/hostname': request.json['options']['conf']['spark.kubernetes.driver.node.selector.kubernetes.io/hostname'],
                    'spark.kubernetes.authenticate.driver.serviceAccountName': request.json['options']['conf']['spark.kubernetes.authenticate.driver.serviceAccountName']
                },
                'additional_options': request.json['additionalOptions'],
                'executable': request.json['options']['executable']
            }
            return json.dumps(dc.create_job(username, status, priority, timestamp, options))
    elif request.method == 'DELETE':
        result = dc.delete_job(job_id)
        if result is None:
            return json.dumps(result), 404
        else:
            return json.dumps(result)
    elif request.method == 'PUT':
        new_options = request.json
        job = dc.retrieve_job(job_id)
        options = job['options']
        # print("\nUser's updates: ")
        # print(new_options)
        # print('\nJob: ')
        # print(job)
        # print('\nJob options: ')
        # print(options)
        for x in new_options:
            options[x] = new_options[x]

        result = dc.update_job(job_id, options)
        if result is None:
            return json.dumps(result), 404
        else:
            return json.dumps(result)
    else:
        return json.dumps({'message': 'Invalid request'}), 400


# Route for user's job
@app.route('/user-jobs/<user>', methods=['GET'])
@authentication_required
def user_jobs(user):
    print('User jobs is running!\n')
    print(f'HEADERS: {request.headers}\n')
    print(user)
    if request.method == 'GET':
        if user is not None:
            return json.dumps(dc.retrieve_jobs(user))
        else:
            result = 'Error 404'
            return json.dumps(result), 404


# Route for user authentication
@app.route('/user-authentication', methods=['POST'])
def user_authentication():
    # Username & Password will be taken from frontend (UI)
    username = ''
    password = ''

    if request.method == 'POST':
        if request.is_json:
            if 'username' in request.json and 'password' in request.json:
                username = request.json['username']
                password = request.json['password']
                if '@pve' not in username:
                    username = username + '@pve'
            if username == '' or password == '':
                return json.dumps({'status': 'error', 'message': 'Invalid request: missing data'}), 401
            else:
                url = f"{proxmox}/api2/json/access/ticket"
                response = requests.post(url, data={"username": username, "password": password}, verify=False)
                auth_res = json.loads(response.text)
                # Permissions request
                # headers = request.headers
                ticket = auth_res['data']['ticket']
                cookie = ('PVEAuthCookie=' + ticket)
                # request.headers.get('Ticket'))
                permission_url = f"{proxmox}/api2/json/access/permissions"
                permission_res = requests.request("GET", permission_url, headers={'Cookie': cookie}, data={}, verify=False)
                if '/vms/299' in json.loads(permission_res.text)['data']:
                    permissions = json.loads(permission_res.text)['data']['/vms/299']
                else:
                    permissions = json.loads(permission_res.text)['data']
                print("Permissions:\n")
                print(permissions)
                rights = 'user'
                for permission in permissions:
                    if 'Sys' in permission:
                        rights = 'admin'
                        break

                if auth_res['data'] is not None:
                    res = {
                        'status': 'success',
                        'rights': rights,
                        'username': auth_res['data']['username'],
                        'ticket': ticket
                    }
                    return json.dumps(res), 200
                else:
                    return json.dumps({'status': 'error', 'message': 'Authentication Error'}), 401
        else:
            return json.dumps({'status': 'error', 'message': 'Invalid request body'}), 401
    else:
        return json.dumps({'status': 'error', 'message': 'Invalid request method'}), 400


# Route for options
@app.route('/retrieve_options', methods=['GET'])
def retrieve_options():
    if request.method == 'GET':
        return json.dumps(dc.retrieve_options()), 200
    else:
        return json.dumps({'status': 'error', 'message': 'Invalid request method'}), 400


# Route for executable file upload, delete and upload
@app.route('/upload_executable', methods=['POST'], defaults={'file': None})
@app.route('/upload_executable/<file>', methods=['PUT', 'DELETE'])
@authentication_required
def upload_executable(file):
    if request.method == 'POST':
        if file is None:
            print('File saving is running...!')
            print(request.files)
            print(request.values)
            # Check if the post request has the file & user part
            if 'spark_exe' not in request.files:
                return json.dumps({'status': 'error', 'message': 'Invalid file type'}), 400
            elif 'user' not in request.values:
                return json.dumps({'status': 'error', 'message': 'No user found'}), 400
            # Load data ('/' is required to user for the right format)
            file = request.files['spark_exe']
            user = request.values['user'] + '/'
            print(f"User: {user}\n")
            # If the user does not select a file, the browser submits an
            # empty file without a filename.
            if file.filename == '':
                return json.dumps({'status': 'error', 'message': 'Empty file'}), 400
            if file and allowed_file(file.filename):
                # Filename
                filename = secure_filename(str(uuid.uuid4()) + '-' + file.filename)
                print(f"Filename: {filename}\n")
                user_folder = os.path.join(app.config['UPLOAD_FOLDER'], user)
                print(f"User's folder: {user_folder}\n")
                # Creating user folder if it's first time
                if not os.path.exists(user_folder):
                    os.mkdir(user_folder)
                file.save(os.path.join(user_folder, filename))
                return json.dumps({'message': f'{filename} saved successfully', 'file_path': f'{os.path.join(user_folder, filename)}', 'filename': filename}), 200
    elif request.method == 'DELETE':
        print('File deleting is running...!')
        print(file)
        for root, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
            if file in files:
                print('Deleting file')
                os.remove(os.path.join(root, file))
                return json.dumps({'message': f'{file} deleted successfully'}), 200
    elif request.method == 'PUT':
        print('File updating is running...!')
        print(file)
    else:
        return json.dumps({'status': 'error', 'message': 'Invalid request method'}), 400


# Route for spark job checking
@app.route('/check_job', methods=['POST'])
def check_job():
    if request.method == 'POST':
        print(request.json)
        job = request.json
        return json.dumps(mo.check_spark_job(job)), 200
    else:
        return json.dumps({'status': 'error', 'message': 'Invalid request method'}), 400


# Route for job's priority change
@app.route('/change_priority/<job_id>', methods=['PUT'])
def change_priority(job_id):
    if request.method == 'PUT':
        new_priority = request.json
        return json.dumps(dc.change_job_priority(job_id, new_priority)), 200
    else:
        return json.dumps({'status': 'error', 'message': 'Invalid request method'}), 400

# Helper: decides where a new pipeline's program and result preview are stored
#
# Both live in the shared folder and share the same unique stem, so the result
# preview of a job can always be found again from the job's executable path:
#   <SHARED_DIR>/programs/<user>/<stem>.py
#   <SHARED_DIR>/results/<stem>.json
def pipeline_paths(user, pipeline_name):
    safe_user = secure_filename(user) or 'user'
    safe_name = secure_filename(pipeline_name) or 'pipeline'
    stem = secure_filename(f'{uuid.uuid4().hex[:12]}-{safe_name}')
    program_path = os.path.join(SHARED_DIR, 'programs', safe_user, stem + '.py')
    return program_path, results_path_for(program_path)


def results_path_for(program_path):
    """ <SHARED_DIR>/programs/<user>/<stem>.py -> <SHARED_DIR>/results/<stem>.json """
    stem = os.path.splitext(os.path.basename(program_path))[0]
    return os.path.join(SHARED_DIR, 'results', stem + '.json')


# Helper: writes a generated program to the shared folder
def save_generated_program(program_path, source_code):
    os.makedirs(os.path.dirname(program_path), exist_ok=True)
    with open(program_path, 'w', encoding='utf-8') as generated_file:
        generated_file.write(source_code)


def driver_pod_name(program_path):
    """
    A fixed, unique name for the driver pod, so the executor can read its log
    and status after spark-submit returns. Pod names must be lower-case
    alphanumerics and '-', at most 63 characters.
    """
    stem = os.path.splitext(os.path.basename(program_path))[0].lower()
    stem = re.sub(r'[^a-z0-9-]+', '-', stem).strip('-')
    return f'{stem[:50].rstrip("-")}-driver'


# Helper: turns the resources part of a pipeline request into the job options dict
#
# 'target' selects where the generated program runs:
#   'kubernetes' (default) - submitted to the (local, Docker Desktop) cluster in
#                            cluster mode: driver and executors run as pods, which
#                            read the program and data from the shared folder
#                            through a hostPath volume
#   'local'                - executed by Spark inside the executor container,
#                            which needs no cluster
def build_job_options(pipeline_name, program_path, resources):
    resources = resources or {}
    target = str(resources.get('target', 'kubernetes')).lower()

    conf = {
        'spark.executor.instances': int(resources.get('instances', 1)),
        'spark.executor.cores': int(resources.get('cores', 1)),
        'spark.executor.memory': int(resources.get('memory', 512)),
        'spark.kubernetes.container.image': resources.get('image') or SPARK_IMAGE,
        'spark.kubernetes.driver.node.selector.kubernetes.io/hostname': resources.get('hostname', K8S_NODE_HOSTNAME),
        'spark.kubernetes.authenticate.driver.serviceAccountName': resources.get('serviceAccount', 'spark')
    }

    if target == 'local':
        master = 'local[*]'
        deploy_mode = 'client'
        executable = program_path
    else:
        master = K8S_MASTER
        deploy_mode = 'cluster'
        # 'local://' tells Spark the file is already inside the pod (it is, through
        # the hostPath volume below), so nothing has to be uploaded
        executable = 'local://' + program_path
        conf['spark.kubernetes.driver.pod.name'] = driver_pod_name(program_path)
        # Use the locally built image, never try to pull it from a registry
        conf['spark.kubernetes.container.image.pullPolicy'] = 'IfNotPresent'
        # The shared folder, mounted at the same path in driver and executor pods
        for role in ('driver', 'executor'):
            prefix = f'spark.kubernetes.{role}.volumes.hostPath.shared'
            conf[f'{prefix}.mount.path'] = SHARED_DIR
            conf[f'{prefix}.options.path'] = SHARED_NODE_DIR

    return {
        'spark_path': resources.get('sparkPath', '/opt/spark/bin/spark-submit'),
        'master': resources.get('master', master),
        'deploy-mode': resources.get('deployMode', deploy_mode),
        'name': pipeline_name,
        # PySpark programs are submitted without --class; the key is kept (and left
        # empty) because the option schema marks it as required.
        'class': '',
        'conf': conf,
        'additional_options': resources.get('additionalOptions', {}),
        'executable': executable
    }


# Helper: a wrong input path is reported before the job is queued, instead of
# failing later on the cluster with PATH_NOT_FOUND.
#   - Kubernetes runs: the pods only see the shared folder, so a local path must
#     lie inside it (URIs such as s3a://... are left alone).
#   - Any path inside the shared folder must exist (the api container sees it too).
def missing_shared_input(spec, target=None):
    path = ((spec or {}).get('input') or {}).get('path')
    if not isinstance(path, str):
        return []
    shared_prefix = SHARED_DIR.rstrip('/') + '/'
    is_local_path = path.startswith('/')
    if target != 'local' and is_local_path and not path.startswith(shared_prefix):
        return [f'input.path: {path!r} is outside the shared folder - Kubernetes pods can only read files under {shared_prefix}']
    if path.startswith(shared_prefix) and not os.path.exists(path):
        return [f'input.path: {path!r} does not exist in the shared folder']
    return []


# Route for previewing the program a specification would generate (nothing is stored)
@app.route('/preview_program', methods=['POST'])
@authentication_required
def preview_program():
    if not request.is_json:
        return json.dumps({'status': 'error', 'message': 'Invalid request body'}), 400

    spec = (request.json or {}).get('spec')
    errors = spg.validate_spec(spec) or missing_shared_input(spec)
    if errors:
        return json.dumps({'status': 'error', 'message': 'Invalid pipeline specification', 'errors': errors}), 400

    source_code = spg.generate_program(spec, skip_validation=True, preview=True)
    return json.dumps({'status': 'success', 'program': source_code}), 200


# Route for creating a pipeline: JSON specification -> generated PySpark program -> job
@app.route('/pipeline', methods=['POST'])
@authentication_required
def pipeline():
    if not request.is_json:
        return json.dumps({'status': 'error', 'message': 'Invalid request body'}), 400

    payload = request.json or {}
    username = payload.get('username')
    spec = payload.get('spec')

    if not username:
        return json.dumps({'status': 'error', 'message': 'No user found'}), 400

    # 1. Validate the specification before anything is written to disk or to the database
    target = str((payload.get('resources') or {}).get('target', 'kubernetes')).lower()
    errors = spg.validate_spec(spec) or missing_shared_input(spec, target)
    if errors:
        return json.dumps({'status': 'error', 'message': 'Invalid pipeline specification', 'errors': errors}), 400

    # 2. Generate the PySpark program from the specification. It also prints a
    #    preview of its result, which the executor stores in the shared folder
    #    for the UI's "Results" button.
    pipeline_name = spec.get('name') or 'pipeline'
    program_path = pipeline_paths(username, pipeline_name)[0]
    source_code = spg.generate_program(spec, skip_validation=True, preview=True)
    filename = os.path.basename(program_path)

    # 3. Store the generated program in the shared folder, where the executor
    #    (local runs) and the driver pod (Kubernetes runs) can both read it
    try:
        save_generated_program(program_path, source_code)
    except OSError as error:
        return json.dumps({'status': 'error', 'message': f'Could not store generated program: {error}'}), 500

    # 4. Register the job, exactly like an uploaded executable would be
    timestamp = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
    options = build_job_options(pipeline_name, program_path, payload.get('resources'))
    report = dc.create_job(username, 'pending', 1, timestamp, options)

    return json.dumps({
        'status': 'success',
        'message': report['message'],
        'filename': filename,
        'file_path': program_path,
        'program': source_code
    }), 200


# Route for the results of a job: the preview the executor stored in the shared
# folder (pipelines only) and the log the executor stored after running it
@app.route('/job-results/<job_id>', methods=['GET'])
@authentication_required
def job_results(job_id):
    try:
        job = dc.retrieve_job(job_id)
    except Exception:
        job = None
    if job is None:
        return json.dumps({'status': 'error', 'message': f'No job with id={job_id} found'}), 404

    executable = job['options'].get('executable', '')
    program_path = executable[len('local://'):] if executable.startswith('local://') else executable

    preview = None
    results_path = os.path.abspath(results_path_for(program_path))
    results_dir = os.path.join(os.path.abspath(SHARED_DIR), 'results') + os.sep
    # Only files inside the shared results folder are ever read
    if results_path.startswith(results_dir) and os.path.isfile(results_path):
        try:
            with open(results_path, encoding='utf-8') as results_file:
                preview = json.load(results_file)
        except (OSError, ValueError) as error:
            print(f'Could not read results {results_path}: {error}')

    log = dc.retrieve_latest_log(job_id)
    return json.dumps({
        'status': 'success',
        'job_status': job.get('status'),
        'name': job['options'].get('name'),
        'preview': preview,
        'log': log['log'] if log else None,
        'log_timestamp': log['timestamp'] if log else None
    }), 200


''' MAIN '''
# Make sure the option definitions exist before serving requests. Without them
# every check against the option schema passes vacuously (see seed_options).
try:
    dc.seed_options()
except Exception as seed_error:
    print(f'\nWARNING: could not seed option definitions: {seed_error}\n')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
