import os
import json
import pandas as pd
import numpy as np
import datetime as dt

class ETL:
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

    def process(self):
        # Extract phase
        df_cases = pd.read_csv(os.path.join(self.path + 'data/cases.csv'), sep = ',', encoding = 'latin_1', quoting = 3)

        df_runners = pd.read_csv(os.path.join(self.path + 'data/runners.csv'), sep = ',')
        df_roles = pd.read_csv(os.path.join(self.path + 'data/roles.csv'), sep = ',')
        df_assistants = pd.read_csv(os.path.join(self.path + 'data/assistants.csv'), sep = ',')
        
        df_ftes = pd.read_csv(os.path.join(self.path + 'data/historical_ftes.csv'), sep = ',')
        
        # Transform phase
        df_ftes = self.rename_columns(df_ftes, 'ftes_columns')
        
        df_runners = self.get_number_machines(df_runners, df_roles)
        
        df_cases = self.adjust_urgency(df_cases)
        
        df_cases = self.join_df(df_cases, df_assistants, df_runners, df_ftes)
        
        df_cases = self.compute_urgencies_percentage(df_cases)
        df_cases = self.compute_sla_compliance(df_cases)
        
        # Load phase
        self.export_df(df_cases)
    
    def rename_columns(self, df, dict_name):
        # Rename DF columns based on a dictionary
        dict = self.params.get(dict_name)
        df = df.rename(columns = dict)
        return df

    def get_number_machines(self, df_runners, df_roles):
        # Get the number of machines by assistant
        df_runners = df_runners.dropna(subset = ['machine']).copy()
        df_runners['machine'] = df_runners['machine'].str.upper()
        df_runners = pd.merge(df_runners, df_roles, on = 'role_id', how = 'inner')
        df_runners = df_runners.groupby(by = 'assistant_id').agg({'machine': 'nunique'}).reset_index()
        return df_runners

    def adjust_urgency(self, df_cases):
        # Converts the urgency of some cases
        urgency_dict = self.params.get('urgency_values')
        df_cases['urgency'] = df_cases['urgency'].replace(urgency_dict)
        return df_cases

    def join_df(self, df_cases, df_assistants, df_runners, df_ftes):
        # Join and process all DFs
        support_groups = self.params.get('support_groups')
        resolved_states = self.params.get('resolved_states')

        df_cases = (df_cases.query("group in @support_groups")
                            .query("state in @resolved_states")
                            .query("summary.str.startswith('RPA')")
                            .query("symptom.str.startswith('ACIS')"))
        df_cases['assistant_nickname'] = df_cases['summary'].str.split(' - ').str[0].str.upper()
        df_cases['symptom'] = df_cases['symptom'].str.replace('?', '', regex = False)
        df_cases['symptom_category'] = df_cases['symptom'].str.split(' - ').str[1]

        df_assistants = (df_assistants.query("assistant_type == 'RPA'")
                                        .query("assistant_state == 'Activo'"))
        df_assistants['assistant_nickname'] = df_assistants['assistant_nickname'].str.upper()
        df_assistants = pd.merge(df_assistants, df_runners, on = 'assistant_id', how = 'inner')

        df_ftes = (df_ftes.query("year == 2022")
                            .query("month in (10, 11, 12)"))
        df_ftes = df_ftes.groupby(by = 'assistant_id').agg({'ftes': 'mean'}).reset_index()

        df_assistants = pd.merge(df_assistants, df_ftes, on = 'assistant_id', how = 'inner')
        df_cases = pd.merge(df_cases, df_assistants, on = 'assistant_nickname', how = 'inner')
        return df_cases 
    
    def compute_urgencies_percentage(self, df_cases):
        # Compute the percentage of cases by urgency for each assistant
        df_urgency_perc = pd.get_dummies(df_cases[['assistant_nickname', 'urgency']], columns = ['urgency'], drop_first = False)

        df_urgency_perc = df_urgency_perc.groupby(by = 'assistant_nickname').agg(
            lambda x: round(sum(x)/len(x), 3)
        ).reset_index()
        
        df_cases = pd.merge(df_cases, df_urgency_perc, on = 'assistant_nickname', how = 'inner')
        return df_cases
    
    def compute_sla_compliance(self, df_cases):
        # Compute the compliance for each case based on the SLA by urgency level
        sla_urgency = self.params.get('sla_urgency')
        df_cases['sla_days'] = df_cases['urgency'].map(sla_urgency)
        df_cases['business_days'] = df_cases.apply(self.get_business_days, axis = 1)
        df_cases['compliance'] = np.where(df_cases['business_days'] <= df_cases['sla_days'], 1, 0)
        return df_cases
    
    def get_business_days(self, df_cases):
        # Get the number of business days needed to resolve each case
        open_date = dt.datetime.strptime(str(df_cases['open_date']), '%Y-%m-%d').date()
        resolved_date = dt.datetime.strptime(str(df_cases['resolved_date']), '%Y-%m-%d').date()
        df_holidays = pd.read_csv(os.path.join(self.path + 'data/holidays.csv'), sep = ',')
        df_holidays['date'] = pd.to_datetime(df_holidays['date'], format = '%d/%m/%Y').dt.date
        holidays_list = df_holidays['date'].tolist()
        business_days = np.busday_count(open_date, resolved_date, holidays = holidays_list)
        return business_days
    
    def export_df(self, df):
        # Load the processed cases DF to output folder
        if os.path.isfile(self.path + 'output/cases_processed.csv'):
            try:
                os.remove(self.path + 'output/cases_processed.csv')
                df.to_csv(self.path + 'output/cases_processed.csv', index = False, encoding = 'latin_1', quoting = 3)
                print('File saved successfully')
            except:
                print('Cannot save file')
        else: 
            try:
                df.to_csv(self.path + 'output/cases_processed.csv', index = False, encoding = 'latin_1', quoting = 3)
                print('File saved successfully')
            except:
                os.mkdir(self.path + 'output')
                df.to_csv(self.path + 'output/cases_processed.csv', index = False, encoding = 'latin_1', quoting = 3)
                print('File saved successfully')