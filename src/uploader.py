import json
import os
from datetime import datetime
import shutil

class Uploader: 
    def __init__(self):
        self.params = self.read_params('params.json')
        
    def read_params(self, file):
        # Read execution parameters
        with open(file) as json_params:
            params = json.load(json_params)
        return params

    def move_to_datalake(self):
        # Create datalake directory if it does not exist
        datalake_path = self.params.get('datalake_path')
        if not os.path.exists(datalake_path):
            os.makedirs(datalake_path)
        # Copy file to datalake
        try:
            in_file = self.params.get('in_file')
            out_file = self.params.get('out_file')
            out_file = out_file + (datetime.now().strftime("%Y-%m-%d_%H_%M_%S")) + '.csv'
            shutil.copy(in_file, out_file)
            print('File moved to datalake successfully')
        except:
            print('Source file does not exist, please run ETL process')