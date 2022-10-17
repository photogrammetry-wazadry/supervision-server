import shutil
import os

import glob, os
import bpy
import shutil
from pathlib import Path, PurePath
import bmesh
import subprocess
import time


if not os.path.exists('export/'):
    os.mkdir('export/')


for dir_name in os.listdir("output/"):
    output_dir = os.path.join("output/", dir_name)
    print(f"Started processing file {dir_name}")
    
    out_path = os.path.join(output_dir, "export.gltf")
    out_path_obj = os.path.join(output_dir, "export.obj")
    in_path = os.path.join(output_dir, "scene.gltf")
    in_path_bin = os.path.join(output_dir, "scene.bin")

    if os.path.exists(out_path) and os.path.exists(in_path):
        bpy.ops.wm.open_mainfile(filepath="template.blend")
        bpy.ops.import_scene.gltf(filepath=in_path, filter_glob='*.glb;*.gltf',
                                          loglevel=0, import_pack_images=True, merge_vertices=False,
                                          import_shading='NORMALS', bone_heuristic='TEMPERANCE',
                                          guess_original_bind_pose=True)

        bpy.ops.export_scene.obj(filepath=os.path.join("export/", dir_name.split('_')[0] + "_input.obj"), 
                check_existing=True, filter_glob='*.obj;*.mtl', use_selection=False, use_animation=False, use_mesh_modifiers=True, 
                use_edges=True, use_smooth_groups=False, use_smooth_groups_bitflags=False, use_normals=True, use_uvs=True, 
                use_materials=True, use_triangles=False, use_nurbs=False, use_vertex_groups=False, use_blen_objects=True, 
                group_by_object=False, group_by_material=False, keep_vertex_order=False, global_scale=1.0, path_mode='AUTO', axis_forward='-Z', axis_up='Y')

        shutil.copy(out_path_obj, os.path.join("export/", dir_name.split('_')[0] + "_output.obj"))
        # shutil.copy(out_path, os.path.join("export/", dir_name.split('_')[0] + "_output.gltf"))
        # shutil.copy(in_path, os.path.join("export/", dir_name.split('_')[0] + "_input.gltf"))
        # shutil.copy(in_path_bin, os.path.join("export/", dir_name.split('_')[0] + "_input.bin"))

        
