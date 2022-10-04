import os

for dir_name in os.listdir("output/"):
    path = os.path.join("output/", dir_name)

    if not os.path.exists(os.path.join(path, "input.zip")):
        print(dir_name)


