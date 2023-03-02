import os
import json
import pandas as pd
import datetime as dt

class Simulator:
    def __init__(self):
        self.path = self.get_folder_path()
        self.params = self.read_params('params.json')

    def get_folder_path(self):
        # Get folder path to read input files
        folder_path = os.path.dirname(os.path.abspath(__file__))
        folder_path = os.path.join(folder_path + '/')
        return folder_path

    def read_params(self, file):
        # Read execution parameters
        with open(file) as json_params:
            params = json.load(json_params)
        return params
    
    def get_cases(self, input_date):
        # Get cases prior to input date and simulate real states based on defined operation parameters
        weeks_backwards = self.params.get('weeks_backwards')
        in_progress_percentage = self.params.get('in_progress_percentage')
        open_percentage = self.params.get('open_percentage')
        
        end_date = dt.datetime.strptime(str(input_date), '%Y-%m-%d').date()
        start_date = end_date - dt.timedelta(weeks = weeks_backwards)
        
        df_cases = pd.read_csv(os.path.join(self.path + 'cases_database/cases.csv'), sep = ',', encoding = 'latin_1', quoting = 3)
        
        df_cases = self.rename_columns(df_cases, 'cases_columns')
        
        df_cases['open_date'] = pd.to_datetime(df_cases['open_date'], format = '%d/%m/%Y').dt.date
        df_cases['resolved_date'] = pd.to_datetime(df_cases['resolved_date'], format = '%d/%m/%Y').dt.date
        
        df_cases = df_cases[df_cases['open_date'] <= end_date]
          
        df_recent_cases = df_cases[df_cases['open_date'] >= start_date]
        
        recent_cases = df_recent_cases['case_id']
        df_past_cases = df_cases.query("case_id not in @recent_cases")
        
        df_in_progress_cases = df_recent_cases.sample(frac = in_progress_percentage)
        df_in_progress_cases['state'] = 'Proceso Solucion'
        df_in_progress_cases['resolved_date'] = ''
        in_progress_cases = df_in_progress_cases['case_id']
        df_recent_cases = df_recent_cases.query("case_id not in @in_progress_cases")
        
        df_open_cases = df_recent_cases.sample(frac = open_percentage)
        df_open_cases['state'] = 'Abierto'
        df_open_cases['resolved_date'] = ''
        open_cases = df_open_cases['case_id']
        df_recent_cases = df_recent_cases.query("case_id not in @open_cases")
        
        df_cases = pd.concat([df_past_cases, df_recent_cases, df_in_progress_cases, df_open_cases])
        
        self.export_df(df_cases)
        
    def rename_columns(self, df, dict_name):
        # Rename DF columns based on a dictionary
        dict = self.params.get(dict_name)
        df = df.rename(columns = dict)
        return df
    
    def export_df(self, df):
        # Load the queried and simulated cases DF to data folder
        if os.path.isfile(self.path + 'data/cases.csv'):
            try:
                os.remove(self.path + 'data/cases.csv')
                df.to_csv(self.path + 'data/cases.csv', index = False, encoding = 'latin_1', quoting = 3)
                print('File saved successfully')
            except:
                print('Cannot save file')
        else: 
            try:
                df.to_csv(self.path + 'data/cases.csv', index = False, encoding = 'latin_1', quoting = 3)
                print('File saved successfully')
            except:
                os.mkdir(self.path + 'data')
                df.to_csv(self.path + 'data/cases.csv', index = False, encoding = 'latin_1', quoting = 3)
                print('File saved successfully')