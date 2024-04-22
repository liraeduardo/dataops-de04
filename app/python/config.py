import os

local_path = os.path.abspath(os.getcwd())
work_path = os.path.dirname(local_path) 

configs = {
      "bronze_path": f"{work_path}\\data\\bronze",
      "meta_path": f"{work_path}\\data",
   }

def project_folders(configs):
   for f in configs.values():
      if not os.path.exists(f):
         os.makedirs(f)