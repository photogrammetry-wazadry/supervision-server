from collections import deque
import os
import pandas as pd
import numpy as np
from flask import Flask, send_from_directory, render_template, send_file, abort, request, flash, redirect, url_for
from dataclasses import dataclass
import shutil
from datetime import datetime
import dataclasses, json
import zipfile
from glob import glob
import subprocess
import time
import filetype

from main import orbit_render, execute
from config import ip_address, port, python_call

# Init app
async_mode = None
app = Flask(__name__, static_url_path='')

log_str = ""

# Handle task database
last_task_id = -1
columns_list = ["id", "filename", "import_datetime", "render_status", "render_start_time", "render_end_time", "render_servername",
                "scan_status", "scan_start_time", "scan_start_time", "scan_servername"]

if not os.path.isfile("tasks.csv"):
    print("No task database found. Generating new")
    df_tasks = pd.DataFrame(columns=columns_list)

    if not os.path.exists("./input/"):
        print("No models to import. Put zip files to ./input or refer to documentation")
        exit(0)

    for filename in os.listdir("./input/"):
        last_task_id += 1
        new_df = pd.DataFrame.from_records([{"id": last_task_id, "filename": '.'.join(filename.split('.')[:-1]),
                                             "import_datetime": datetime.now().strftime("%m.%d.%Y_%H:%M:%S"),
                                             "render_status": "none", "scan_status": "none"}], columns=columns_list)

        # df_tasks.loc[last_task_id] = new_df
        df_tasks = pd.concat([df_tasks.loc[:], new_df]).reset_index(drop=True)

    df_tasks.to_csv("tasks.csv", index=False)
    print(f"Imported {last_task_id + 1} 3D models\n")

else:
    df_tasks = pd.read_csv("tasks.csv", index_col=False)
    # print(df_tasks)

    last_task_id = max(df_tasks["id"])
    print("Loaded task database. Looking for new models")
    df_tasks["render_status"] = df_tasks["render_status"].replace("processing", "none")
    df_tasks["scan_status"] = df_tasks["scan_status"].replace("processing", "none")

    new_count = 0
    for filename in os.listdir("./input/"):
        if '.'.join(filename.split('.')[:-1]) not in list(df_tasks["filename"]):
            last_task_id += 1
            new_count += 1
            new_df = pd.DataFrame({"id": last_task_id, "filename": '.'.join(filename.split('.')[:-1]),
                                                 "import_datetime": datetime.now().strftime("%m.%d.%Y_%H:%M:%S"),
                                                 "render_status": "none", "scan_status": "none"}, index=[0])

            df_tasks = pd.concat([df_tasks.loc[:], new_df]).reset_index(drop=True)

    df_tasks.to_csv("tasks.csv", index=False)
    if new_count > 0:
        print(f"Imported {new_count} new 3D models\n")


server_busy = False
task_workers = dict()  # {server name: ImageTask, task start datetime}


@dataclass
class Task:
    id: int
    name: str
    type: str
    start_time: str


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


def change_status(task, new_status):
    df_tasks[df_tasks["id"] == task.id][task.type + "_status"] = new_status
    df_tasks.to_csv("tasks.csv", index=False)


def task_get(task_type, new_status, server_name):
    task = df_tasks[df_tasks[task_type + "_status"] == "none"].iloc[0]

    df_tasks.iloc[task.id, columns_list.index(task_type + "_start_time")] = datetime.now().strftime("%m.%d.%Y_%H:%M:%S")
    df_tasks.iloc[task.id, columns_list.index(task_type + "_status")] = new_status
    df_tasks.iloc[task.id, columns_list.index(task_type + "_servername")] = server_name

    df_tasks.to_csv("tasks.csv", index=False)
    return Task(int(task["id"]), task["filename"], task_type, task[task_type + "_start_time"])


# Return main page
@app.route('/')
def root():
    return df_tasks.to_html()


@app.route('/logs')
def logs():
    return log_str


@app.route('/workers')
def get_info():
    return json.dumps(task_workers, cls=EnhancedJSONEncoder)


@app.route('/disconnect/<name>')
def disconnect(name):
    if name not in task_workers.keys():
        return json.dumps({'success': False}), 400, {'ContentType': 'application/json'}

    task = task_workers[name]

    change_status(task, "none")
    task_workers.pop(name, None)

    return json.dumps({'success': True}), 200, {'ContentType': 'application/json'}


@app.route('/disconnect_all')
def disconnect_all():
    locked_tasks = task_workers
    for name in locked_tasks:
        disconnect(name)

    return json.dumps({'success': True}), 200, {'ContentType': 'application/json'}


@app.route('/skip/<name>')
def skip(name):
    if name in task_workers.keys():
        print(f"Skipping model {task_workers[name]['task'].name} for server {name}")
        change_status(task_workers[name], "skip")
        task_workers.pop(name, "none")

        return json.dumps({'success': True}), 200, {'ContentType': 'application/json'}

    else:
        return json.dumps({'success': False}), 400, {'ContentType': 'application/json'}


def send_model(name, task=None):
    global log_str, server_busy

    print("Starting model send")
    if task is None:
        task = task_get("render", "processing", name)
        print(task)

    task_workers[name] = task

    output_dir = os.path.join("output/", str(task.id).zfill(5))
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    else:
        for file_name in glob(os.path.join(output_dir, "*")):
            os.remove(file_name)

    output_blend_file = os.path.join(output_dir, "project.blend")

    # TODO: rewrite this function call via shell command
    cmd = f'{python_call} -c "import bpy; from main import orbit_render; orbit_render(\'{task.name}.zip\')"'
    popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        print(stdout_line, end='')
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)

    try:
        mimetype = filetype.guess_mime("project.blend")
        response = send_file("project.blend", as_attachment=True, mimetype=mimetype)
        response.headers["task_type"] = "render"  # Image

        info = f"[{datetime.now().strftime('%m.%d.%Y_%H:%M:%S')}] Gave task {task.name} for render to server: {name}"
        print(info)
        log_str += info + '\n'

        shutil.move("project.blend", output_blend_file)
        server_busy = False

        return response
    except FileNotFoundError:
        abort(404)


def send_images(name, task=None):
    global log_str, server_busy

    print("Starting image send")
    if task is None:
        task = task_get("scan", "processing", name)
    task_workers[name] = task

    folder_path = os.path.join("./output/", str(task.id).zfill(5))
    print(f"Archive started, path {folder_path}")
    shutil.make_archive("photos", 'zip', folder_path)
    print("Archive finished")

    try:
        mimetype = filetype.guess_mime("photos.zip")
        response = send_file("photos.zip", as_attachment=True, mimetype=mimetype)
        response.headers["task_type"] = "model"  # Model

        info = f"[{datetime.now().strftime('%m.%d.%Y_%H:%M:%S')}] Gave task {task.name} for modeling to server: {name}"
        print(info)
        log_str += info + '\n'
        server_busy = False

        return response
    except FileNotFoundError:
        abort(404)


@app.route('/get_task/<name>/<can_do_images>/<can_do_models>')  # Client event to get new task
def get_task(name, can_do_images, can_do_models):
    global server_busy
    while server_busy:
        print("Archive in progress, waiting in line")
        time.sleep(60)

    server_busy = True
    can_do_images = can_do_images == "true"
    can_do_models = can_do_models == "true"

    task = None
    if name in task_workers.keys():
        print("Job for this client already found, restoring last request")
        task = task_workers[name]

    if can_do_images and can_do_models:
        print(df_tasks[df_tasks["render_status"] != "completed"]["id"])
        if min(df_tasks[df_tasks["render_status"] != "completed"]["id"]) >= min(
                df_tasks[df_tasks["scan_status"] != "completed"]["id"]):
            # Return model to client
            return send_model(name, task)
        else:
            # Return images to client
            return send_images(name, task)

    elif can_do_images:
        return send_model(name, task)

    elif can_do_models:
        return send_images(name, task)


@app.route('/submit_task/<name>/<task_type>', methods=['GET', 'POST'])  # Client event to return finished work
def submit_task(name, task_type):
    global df_tasks

    if request.method == 'POST':
        task = task_workers[name]
        output_dir = os.path.join("output/", str(task.id).zfill(5))

        if task_type == "render":
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            else:
                for file_name in glob(os.path.join(output_dir, "*.png")):
                    os.remove(file_name)

        file = request.files['file']
        full_file_path = os.path.join(output_dir, file.filename)

        file.save(full_file_path)

        # Extract
        with zipfile.ZipFile(full_file_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)

        df_tasks.iloc[task.id, columns_list.index(task_type + "_end_time")] = datetime.now().strftime(
            "%m.%d.%Y_%H:%M:%S")
        df_tasks.iloc[task.id, columns_list.index(task_type + "_status")] = "completed"

        df_tasks.to_csv("tasks.csv", index=False)
        task_workers.pop(name, None)
        print(task_workers)

    return json.dumps({'success': True}), 200, {'ContentType': 'application/json'}


# Get files from server (e.g libs)
@app.route('/js/<path:path>')
def send_js(path):
    return send_from_directory('js', path)


if __name__ == "__main__":
    # Create needed directories
    needed_dirs = ["done/", "input/", "output/", "temp/"]
    for needed_dir in needed_dirs:
        if not os.path.exists(needed_dir):
            os.mkdir(needed_dir)

    app.secret_key = 'token'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=ip_address, port=port)
