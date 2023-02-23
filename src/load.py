import shutil
from datetime import datetime
import json

class cargar: 


    def read_params(self, file):
        with open(file) as json_params:
            self.params = json.load(json_params)

        return self.params

    def copy_file(self):
        self.params = self.read_params('params.json')
        in_file = self.params.get('in_file')
        out_file = self.params.get('out_file')
        out_file = out_file + (datetime.now().strftime("%Y-%m-%d_%H_%M_%S.%f")) + '.csv'
        shutil.copy(in_file, out_file)

 
