""" Backend Python Script for job execution service """

''' LIBRARIES '''
import subprocess   # Running spark submit
import time     # Timings
import DatabaseConnection as dc
import SparkCommandGenerator as scg    # JSON -> spark-submit command
import os
import shutil   # Copying the sample datasets into the shared folder
import json     # Validating the result preview printed by a pipeline

''' CONSTANTS '''
# Shared folder (hostPath): mounted here by docker-compose and, at the same
# path, in every driver/executor pod. Programs, data and result previews live there.
SHARED_DIR = os.environ.get('SHARED_DIR', '/mnt/shared')
# Sample datasets baked into the image, copied into the shared folder so that
# pods can read them too: {path in the image: path relative to SHARED_DIR}
SAMPLE_DATASETS = {
    '/data/customers.csv': 'data/customers.csv',
    '/data/reviews.csv': 'data/reviews.csv',
}
# A MongoDB document is limited to 16 MB; Spark logs can be long, so only the
# end of a log (where errors and results are) is kept
LOG_LIMIT = 200000
# Lines that enclose the result preview in a generated program's output - must
# match PREVIEW_BEGIN / PREVIEW_END in backend/SparkProgramGenerator.py
PREVIEW_BEGIN = '=====SPARK-GUI-PREVIEW-BEGIN====='
PREVIEW_END = '=====SPARK-GUI-PREVIEW-END====='

''' FUNCTIONS '''
# Creates the shared folder layout and copies the sample datasets into it
def prepare_shared_folder():
    if not os.path.isdir(SHARED_DIR):
        print(f'\nWARNING: shared folder {SHARED_DIR} is not mounted; Kubernetes pipelines will not find their files\n')
        return
    for sub_folder in ('programs', 'results', 'data'):
        os.makedirs(os.path.join(SHARED_DIR, sub_folder), exist_ok=True)
    for source, relative_target in SAMPLE_DATASETS.items():
        target = os.path.join(SHARED_DIR, relative_target)
        if os.path.isfile(source) and not os.path.exists(target):
            shutil.copyfile(source, target)
            print(f'Sample dataset copied to {target}')

# Select the best job according to priority and timestamp (The Lowest Priority and FirstCameFirstServed)
def job_selector():
    # Retrieving pending jobs from database
    try:
        jobs_to_execute = dc.retrieve_jobs_for_execution()
        print(f'Before Sorting (or without): \n{jobs_to_execute}\n')
        return jobs_to_execute[0]
    except Exception as e:
        print(e)
    # for job in jobs_to_execute:
    #     print(f"{job['timestamp']}\n")
    # Sorting jobs from newest to oldest
    # jobs_to_execute = sorted(jobs_to_execute, key=lambda d: d['timestamp'], reverse=True)
    # print(f'After Sorting: \n{jobs_to_execute}\n')
    # if len(jobs_to_execute) == 0:
    #     for job in jobs_to_execute:
    #         # print(f"{job['timestamp']}\n")
    #         job_executor(job)
    # else:
    #     print('No jobs to execute\n')

# Runs kubectl with the given arguments (returns the CompletedProcess)
def kubectl(*args):
    try:
        return subprocess.run(['kubectl', *args], shell=False, capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.TimeoutExpired) as error:
        return subprocess.CompletedProcess(args, 1, '', str(error))

# Name of the driver pod, if the job fixed one (pipelines submitted to Kubernetes do)
def driver_pod_of(options):
    if not str(options.get('master', '')).startswith('k8s://'):
        return None
    return (options.get('conf') or {}).get('spark.kubernetes.driver.pod.name')

# Keeps only the end of a long log
def trim_log(text):
    if len(text) <= LOG_LIMIT:
        return text
    return '... (log truncated) ...\n' + text[-LOG_LIMIT:]

# Where the result preview of a generated program is kept (None for other jobs):
# <SHARED_DIR>/programs/<user>/<stem>.py -> <SHARED_DIR>/results/<stem>.json
def results_path_of(options):
    executable = str(options.get('executable', ''))
    if executable.startswith('local://'):
        executable = executable[len('local://'):]
    programs_dir = os.path.join(SHARED_DIR, 'programs') + os.sep
    if not os.path.abspath(executable).startswith(programs_dir):
        return None
    stem = os.path.splitext(os.path.basename(executable))[0]
    return os.path.join(SHARED_DIR, 'results', stem + '.json')

# Finds the preview a generated program printed, stores it as a JSON file in the
# shared folder (the UI reads it from there) and returns the log without it.
# The driver pod cannot write that file itself (PermissionError on the shared
# Windows folder, see PREVIEW_BEGIN in SparkProgramGenerator.py); this container can.
def store_preview(options, log):
    start = log.find(PREVIEW_BEGIN)
    end = log.find(PREVIEW_END, start + 1) if start != -1 else -1
    results_path = results_path_of(options)
    if start == -1 or end == -1 or results_path is None:
        return log
    payload = log[start + len(PREVIEW_BEGIN):end].strip()
    try:
        json.loads(payload)    # never store something the UI cannot read
        os.makedirs(os.path.dirname(results_path), exist_ok=True)
        temporary = results_path + '.tmp'
        with open(temporary, 'w', encoding='utf-8') as preview_file:
            preview_file.write(payload)
        os.replace(temporary, results_path)    # the UI never sees a half-written file
        note = f'(result preview stored in {results_path})'
    except (ValueError, OSError) as error:
        note = f'WARNING: could not store the result preview: {error}'
    print(note)
    return log[:start] + note + log[end + len(PREVIEW_END):]

# Edit & Run job (returns True if the job succeeded)
def job_executor(job):
    print(f"{job['options']}\n")

    # Building the spark-submit command (validated against the DB option
    # definitions) as a list of args - no shell=True, no manual string
    # concatenation.
    try:
        spark_submit_args = scg.generate_spark_submit_args(job['options'])
    except scg.SparkCommandValidationError as e:
        message = f"Invalid job options: {e.errors}"
        print(f"\n{message}\n")
        dc.add_log(job, message)
        return False

    print(f"\nCommand:\n{scg.to_display_string(spark_submit_args)}\n")

    # A driver pod left over from an earlier attempt would block the new one
    driver_pod = driver_pod_of(job['options'])
    if driver_pod:
        kubectl('delete', 'pod', driver_pod, '--ignore-not-found=true', '--wait=true')

    # Run job
    cwd = os.getcwd()
    print(f'\nCurrent Path: {cwd}\n')
    result = subprocess.run(spark_submit_args, shell=False, capture_output=True, text=True)
    print(result.stderr)

    if driver_pod:
        # Cluster mode: spark-submit only reports the submission. The program's
        # own output (and its success or failure) belongs to the driver pod.
        phase = kubectl('get', 'pod', driver_pod, '-o', 'jsonpath={.status.phase}').stdout.strip()
        pod_log = kubectl('logs', driver_pod)
        succeeded = phase == 'Succeeded'
        log = pod_log.stdout if pod_log.returncode == 0 else pod_log.stderr
        if not succeeded:
            log += f'\n\n--- driver pod phase: {phase or "unknown"} ---\n--- spark-submit output ---\n{result.stderr}'
    else:
        # Local (or client mode) run: the program's output is spark-submit's output
        succeeded = result.returncode == 0
        log = result.stdout
        if not succeeded or not log.strip():
            log += f'\n\n--- spark-submit output (exit code {result.returncode}) ---\n{result.stderr}'

    log = store_preview(job['options'], log)
    dc.add_log(job, trim_log(log))
    return succeeded

# cluster_info = 'kubectl cluster-info dump'
# spark_submit = "/opt/spark/bin/spark-submit \
#   --master k8s://https://192.168.0.201:16443 \
#   --deploy-mode cluster \
#   --name spark-pi \
#   --class org.apache.spark.examples.SparkPi \
#   --conf spark.executor.instances=1 \
#   --conf spark.executor.cores=1 \
#   --conf spark.executor.memory=1g \
#   --conf spark.kubernetes.driver.node.selector.kubernetes.io/hostname=k8s-1 \
#   --conf spark.kubernetes.container.image=apache/spark:3.5.0 \
#   --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
#   local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar \
#   1000"

''' GLOBAL '''
prepare_shared_folder()

# Looping: Selecting job, Executing job, Waiting
stop = False
while stop is False:
    start_time = time.time()
    job = job_selector()    # Selecting job
    print(f'\nJob for execution:\n{job}\n')
    if not job:  # Case no job exists or all jobs are finished
        # stop = True
        print(f'\nNo job found for execution\n')
    else:
        dc.edit_timestamp(job, 'started', 'add')
        succeeded = job_executor(job)     # Executing job
        end_time = time.time()
        print(f'\nTime: {str(end_time - start_time)} secs\n')
        print(f'\nExecution succeeded: {succeeded}\n')
        if succeeded:
            dc.edit_timestamp(job, 'finished', 'add')
        else:
            dc.edit_timestamp(job, 'started', 'remove')     # marks the job as failed
    time.sleep(10)  # Waiting until next execution
