from collections import deque
import os
import pandas as pd
from flask import Flask, send_from_directory, render_template, send_file, abort, request, flash, redirect, url_for
from dataclasses import dataclass
import shutil
from datetime import datetime
import json
import zipfile
from glob import glob
import subprocess
import time
import filetype

from main import orbit_render, execute
from ip import ip_address, port


# Init app
async_mode = None
app = Flask(__name__, static_url_path='')

log_str = ""
if not os.path.isfile("completed_tasks.csv"):
    df_completed_tasks = pd.DataFrame(columns=["id", "datetime", "servername", "task_name", "start_index"])
else:
    df_completed_tasks = pd.read_csv("completed_tasks.csv")

server_busy = False
image_queue = deque()  # Queue to generate images of model
model_queue = deque()  # Queue to generate model from images
skip_queue = deque()   # Queue for all tasks that got and exception of some kind
task_workers = dict()  # {server name: ImageTask, task start datetime}


@dataclass
class Task:
    id: int
    name: str
    task_type: str
    folder_name: str
    start_index: int


# Return main page
@app.route('/')
def root():
    return task_workers


@app.route('/logs')
def logs():
    return log_str


@app.route('/get_info')
def get_info():
    return task_workers


@app.route('/image_queue')
def return_image_queue():
    return list(image_queue)


@app.route('/model_queue')
def return_model_queue():
    return list(model_queue)


@app.route('/skip_queue')
def return_skip_queue():
    return list(skip_queue)


@app.route('/disconnect/<name>')
def disconnect(name):
    if name not in task_workers.keys():
        return json.dumps({'success': False}), 400, {'ContentType': 'application/json'}

    task = task_workers[name]["task"]

    if task.task_type == "render":
        shutil.move(os.path.join("processing/", task.name + ".zip"), os.path.join("input/", task.name + ".zip"))
        image_queue.appendleft(task)
    else:
        model_queue.appendleft(task)
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
        skip_queue.append(task_workers[name]["task"])
        task_workers.pop(name, None)

        return json.dumps({'success': True}), 200, {'ContentType': 'application/json'}

    else:
        return json.dumps({'success': False}), 400, {'ContentType': 'application/json'}


def send_model(name, task=None):
    global log_str, server_busy
    
    print("Starting model send")
    if task is None:
        task = image_queue.popleft()

    task_workers[name] = {"task": task, "start_time": datetime.now().strftime("%m.%d.%Y_%H:%M:%S")}
    file_path = os.path.join("./input/", task.name + ".zip")

    output_dir = os.path.join("output/", task.folder_name)
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    else:
        for file_name in glob(os.path.join(output_dir, "*")):
            os.remove(file_name)

    output_blend_file = os.path.join(output_dir, "project.blend")

    # TODO: rewrite this function call via shell command
    cmd = f'python3.10 -c "import bpy; from main import orbit_render; orbit_render(\'{task.name}.zip\')"'
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
        response.headers["start_index"] = task.start_index
        response.headers["task_type"] = "render"  # Image

        info = f"[{datetime.now().strftime('%m.%d.%Y_%H:%M:%S')}] Gave task {task.name} for render to server: {name}"
        print(info)
        log_str += info + '\n'

        shutil.move(file_path, os.path.join("./processing/", task.name + ".zip"))
        shutil.move("project.blend", output_blend_file)
        server_busy = False

        return response
    except FileNotFoundError:
        abort(404)


def send_images(name, task=None):
    global log_str, server_busy
    
    print("Starting image send")
    if task is None:
        task = model_queue.popleft()
    task_workers[name] = {"task": task, "start_time": datetime.now().strftime("%m.%d.%Y_%H:%M:%S")}
   

    folder_path = os.path.join("./output/", task.folder_name)
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
        task = task_workers[name]["task"]

    if can_do_images and can_do_models:
        if len(image_queue) >= len(model_queue):
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
    global df_completed_tasks

    if request.method == 'POST':
        task = task_workers[name]["task"]
        output_dir = os.path.join("output/", task.folder_name)
        
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
        
        if task_type == "render":
            shutil.move(os.path.join("./processing/", task.name + ".zip"),
                        os.path.join("./done/", task.name + ".zip"))
        
        df_completed_tasks = df_completed_tasks.append(
            {"id": task.id, "datetime": datetime.now().strftime("%m.%d.%Y_%H:%M:%S"),
             "servername": name, "task_name": task.name, "task_dir": task.folder_name,
             "start_index": task.start_index}, ignore_index=True)
        df_completed_tasks.to_csv("completed_tasks.csv")
        task_workers.pop(name, None)
        print(task_workers)

    return json.dumps({'success': True}), 200, {'ContentType': 'application/json'}


# Get files from server (e.g libs)
@app.route('/js/<path:path>')
def send_js(path):
    return send_from_directory('js', path)


if __name__ == "__main__":
    # Create needed directories
    needed_dirs = ["done/", "input/", "output/", "processing/", "temp/"]
    for needed_dir in needed_dirs:
        if not os.path.exists(needed_dir):
            os.mkdir(needed_dir)

    # Move all files that have been in progress to input dir
    for filename in os.listdir("./processing/"):
        path = os.path.join('./processing/', filename)
        shutil.move(path, "./input/")

    # Parse last model stopped on
    undone_indexes = list(range(1, 10000))
    for dirname in os.listdir("./output"):
        if len(os.listdir(os.path.join("./output", dirname))) > 1:
            print(dirname)
            undone_indexes.remove(int(dirname.split('_')[0]))

    model_index = 0
    print(f"Starting on index {undone_indexes[model_index]}")
    print(undone_indexes[:10])

    # Parse input files and push them to queue (init queue)
    for filename in os.listdir("./input/"):
        path = os.path.join('./input/', filename)  # Relative path to input

        no_extension_name = os.path.splitext(filename)[0]
        render_folder_name = f"{str(undone_indexes[model_index]).zfill(4)}_{no_extension_name}"  # Folder name with index
        output_dir = os.path.join("output/", render_folder_name)  # Relative output path

        image_queue.append(Task(name=no_extension_name, folder_name=render_folder_name,
                                start_index=1, id=undone_indexes[model_index], task_type="render"))
        model_index += 1
    
    model_index = 0
    for folder_name in os.listdir("output/"):
        if not os.path.exists(os.path.join("output/", folder_name, "model.zip")):
            model_queue.append(Task(name='_'.join(folder_name.split("_")[1:]), folder_name=folder_name,
                                    start_index=1, id=model_index, task_type="model"))
        model_index += 1

    app.secret_key = 'token'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=ip_address, port=port)
