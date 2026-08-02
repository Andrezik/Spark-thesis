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

# Global Variables
proxmox = os.environ['PROXMOX']  # Dockerfile Configuration
ALLOWED_EXTENSIONS = {'jar', 'py', 'json'}   # txt must be removed
# Instance of flask application
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = './backend/uploads/'  # Dockerfile Configuration
CORS(app)

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

# Helper: writes a generated program to the user's upload folder and returns its path
def save_generated_program(user, pipeline_name, source_code):
    user_folder = os.path.join(app.config['UPLOAD_FOLDER'], user + '/')
    if not os.path.exists(user_folder):
        os.makedirs(user_folder)
    safe_name = secure_filename(pipeline_name) or 'pipeline'
    filename = secure_filename(f'{uuid.uuid4()}-{safe_name}.py')
    # Absolute path: UPLOAD_FOLDER is relative ('./backend/uploads/') and would
    # only resolve correctly from the API container's working directory. The
    # executor runs from a different directory, so it needs the full path.
    file_path = os.path.abspath(os.path.join(user_folder, filename))
    with open(file_path, 'w', encoding='utf-8') as generated_file:
        generated_file.write(source_code)
    return file_path, filename


# Helper: turns the resources part of a pipeline request into the job options dict
#
# 'target' selects where the generated program runs:
#   'kubernetes' (default) - submitted to the cluster, the original behaviour
#   'local'                - executed by Spark inside the executor container,
#                            which needs no cluster and is used for demonstrating
#                            the full chain end to end
def build_job_options(pipeline_name, program_path, resources):
    resources = resources or {}
    target = str(resources.get('target', 'kubernetes')).lower()

    if target == 'local':
        default_master = 'local[*]'
        default_deploy_mode = 'client'
    else:
        default_master = 'k8s://https://kubernetes.default.svc'
        default_deploy_mode = 'cluster'

    return {
        'spark_path': resources.get('sparkPath', '/opt/spark/bin/spark-submit'),
        'master': resources.get('master', default_master),
        'deploy-mode': resources.get('deployMode', default_deploy_mode),
        'name': pipeline_name,
        # PySpark programs are submitted without --class; the key is kept (and left
        # empty) because the option schema marks it as required.
        'class': '',
        'conf': {
            'spark.executor.instances': int(resources.get('instances', 1)),
            'spark.executor.cores': int(resources.get('cores', 1)),
            'spark.executor.memory': int(resources.get('memory', 512)),
            'spark.kubernetes.container.image': resources.get('image', 'apache/spark:3.5.0'),
            'spark.kubernetes.driver.node.selector.kubernetes.io/hostname': resources.get('hostname', 'k8s-1'),
            'spark.kubernetes.authenticate.driver.serviceAccountName': resources.get('serviceAccount', 'spark')
        },
        'additional_options': resources.get('additionalOptions', {}),
        'executable': program_path
    }


# Route for previewing the program a specification would generate (nothing is stored)
@app.route('/preview_program', methods=['POST'])
@authentication_required
def preview_program():
    if not request.is_json:
        return json.dumps({'status': 'error', 'message': 'Invalid request body'}), 400

    spec = (request.json or {}).get('spec')
    errors = spg.validate_spec(spec)
    if errors:
        return json.dumps({'status': 'error', 'message': 'Invalid pipeline specification', 'errors': errors}), 400

    source_code = spg.generate_program(spec, skip_validation=True)
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
    errors = spg.validate_spec(spec)
    if errors:
        return json.dumps({'status': 'error', 'message': 'Invalid pipeline specification', 'errors': errors}), 400

    # 2. Generate the PySpark program from the specification
    source_code = spg.generate_program(spec, skip_validation=True)
    pipeline_name = spec.get('name') or 'pipeline'

    # 3. Store the generated program so the executor can submit it
    try:
        program_path, filename = save_generated_program(username, pipeline_name, source_code)
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


''' MAIN '''
# Make sure the option definitions exist before serving requests. Without them
# every check against the option schema passes vacuously (see seed_options).
try:
    dc.seed_options()
except Exception as seed_error:
    print(f'\nWARNING: could not seed option definitions: {seed_error}\n')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
