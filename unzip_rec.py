import zipfile
import os


for dir_name in os.listdir("output/"):
    output_dir = os.path.join("output/", dir_name)
    print(f"Started processing file {dir_name}")

    if not os.path.exists(os.path.join(output_dir, "export.gltf")) and os.path.exists(os.path.join(output_dir, "model.zip")):
        with zipfile.ZipFile(os.path.join(output_dir, "model.zip"), 'r') as zip_ref:
            zip_ref.extractall(output_dir)

    if not os.path.exists(os.path.join(output_dir, "input.gltf")) and os.path.exists(os.path.join(output_dir, "input.zip")):
        with zipfile.ZipFile(os.path.join(output_dir, "input.zip"), 'r') as zip_ref:
            zip_ref.extractall(output_dir)

