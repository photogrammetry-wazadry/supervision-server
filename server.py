from collections import deque
import os
import pandas as pd
import glob
from flask import Flask, send_from_directory, render_template, send_file, abort, request, flash, redirect, url_for
from dataclasses import dataclass
import shutil
from datetime import datetime
import json
import zipfile

from ip import ip_address, port

# Init app
async_mode = None
app = Flask(__name__, static_url_path='')

log_str = ""
if not os.path.isfile("completed_tasks.csv"):
    df_completed_tasks = pd.DataFrame(columns=["id", "datetime", "servername", "task_name", "start_index"])
else:
    df_completed_tasks = pd.read_csv("completed_tasks.csv")

image_queue = deque()  # Queue to generate images of model
model_queue = deque()  # Queue to generate model from images
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


@app.route('/disconnect/<name>')
def disconnect(name):
    task = task_workers[name]["task"]
    shutil.move(os.path.join("processing/", task.name + ".zip"), os.path.join("input/", task.name + ".zip"))

    if task.task_type == "render":
        image_queue.appendleft(task)
    else:
        model_queue.appendleft(task)
    task_workers.pop(name, None)

    return json.dumps({'success': True}), 200, {'ContentType': 'application/json'}


def send_model(name):
    global log_str

    task = image_queue.popleft()
    task_workers[name] = {"task": task, "start_time": datetime.now().strftime("%m.%d.%Y_%H:%M:%S")}
    file_path = os.path.join("./input/", task.name + ".zip")

    try:
        response = send_file(file_path, as_attachment=True)
        response.headers["start_index"] = task.start_index
        response.headers["task_type"] = "render"  # Image

        info = f"[{datetime.now().strftime('%m.%d.%Y_%H:%M:%S')}] Gave task {task.name} render to server: {name}"
        print(info)
        log_str += info + '\n'

        shutil.move(file_path, os.path.join("./processing/", task.name + ".zip"))
        return response
    except FileNotFoundError:
        abort(404)


def send_images(name):
    global log_str

    task = model_queue.popleft()
    task_workers[name] = [task, datetime.now().strftime("%m.%d.%Y_%H:%M:%S")]

    folder_path = os.path.join("./output/", task.folder_name)
    shutil.make_archive(folder_path, 'zip', folder_path)
    archive_path = os.path.join(folder_path + '.zip')
    try:
        response = send_file(archive_path, as_attachment=True)
        response.headers["task_type"] = "model"  # Model

        info = f"[{datetime.now().strftime('%m.%d.%Y_%H:%M:%S')}] Gave task {task.name} model to server: {name}"
        print(info)
        log_str += info + '\n'

        return response
    except FileNotFoundError:
        abort(404)


@app.route('/get_task/<name>/<can_do_images>/<can_do_models>')  # Client event to get new task
def get_task(name, can_do_images, can_do_models):
    can_do_images = can_do_images == "true"
    can_do_models = can_do_models == "true"

    if can_do_images and can_do_models:
        if len(image_queue) >= len(model_queue):
            # Return model to client
            return send_model(name)
        else:
            # Return images to client
            return send_images(name)

    elif can_do_images:
        return send_model(name)

    elif can_do_models:
        return send_images(name)


@app.route('/submit_task/<name>/<task_type>', methods=['GET', 'POST'])  # Client event to return finished work
def submit_task(name, task_type):
    global df_completed_tasks

    if request.method == 'POST':
        if task_type == "render":
            file = request.files['file']

            if file:
                task = task_workers[name]["task"]
                output_dir = os.path.join("output/", task.folder_name)

                if not os.path.exists(output_dir):
                    os.mkdir(output_dir)
                else:
                    for file_name in glob.glob(os.path.join(output_dir, "*")):
                        os.remove(file_name)

                file.save(os.path.join(output_dir, file.filename))

                # Extract
                with zipfile.ZipFile(os.path.join(output_dir, "render.zip"), 'r') as zip_ref:
                    zip_ref.extractall(output_dir)

                shutil.move(os.path.join("./processing/", task.name + ".zip"),
                            os.path.join("./done/", task.name + ".zip"))
                df_completed_tasks = df_completed_tasks.append(
                    {"id": task.id, "datetime": datetime.now().strftime("%m.%d.%Y_%H:%M:%S"),
                     "servername": name, "task_name": task.name, "task_dir": task.folder_name,
                     "start_index": task.start_index}, ignore_index=True)
                df_completed_tasks.to_csv("completed_tasks.csv")

        elif task_type == "model":
            pass  # Save post request's model to output directory

    return json.dumps({'success': True}), 200, {'ContentType': 'application/json'}


# Get files from server (e.g libs)
@app.route('/js/<path:path>')
def send_js(path):
    return send_from_directory('js', path)


if __name__ == "__main__":
    # Create needed directories
    needed_dirs = ["done/", "input/", "output/", "processing/"]
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
        undone_indexes.remove(int(dirname.split('_')[0]) + 1)

    model_index = 0
    print(f"Starting on index {undone_indexes[model_index]}")

    # Parse input files and push them to queue (init queue)
    for filename in os.listdir("./input/"):
        path = os.path.join('./input/', filename)  # Relative path to input

        no_extension_name = os.path.splitext(filename)[0]
        render_folder_name = f"{str(undone_indexes[model_index]).zfill(4)}_{no_extension_name}"  # Folder name with index
        output_dir = os.path.join("output/", render_folder_name)  # Relative output path

        # files = glob.glob(os.path.join(output_dir, "*.png"))
        # last_rendered_index = 0

        # if len(files) != 0:
        #     last_rendered = sorted(files)[-1]
        #     last_rendered_index = int(os.path.splitext(PurePath(last_rendered).parts[-1])[0])

        # if last_rendered_index == 300:
        #     print(f"skipped {render_folder_name}. already rendered")
        #     continue

        image_queue.append(Task(name=no_extension_name, folder_name=render_folder_name,
                                start_index=1, id=undone_indexes[model_index], task_type="render"))
        model_queue.append(Task(name=no_extension_name, folder_name=render_folder_name,
                                start_index=1, id=undone_indexes[model_index], task_type="model"))
        model_index += 1

        # TODO: model queue process

    app.secret_key = 'token'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=ip_address, port=port)
