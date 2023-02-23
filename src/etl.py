import pandas as pd
import os


class ExtractTransformLoad():

    def ETL(self):
        ###FASE DE EXTRACCION
        
        #Obtener la ruta de la carpeta para leer archivos
        path = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(path + '/')
        
        #Leer los archivos 
        df_cases = pd.read_csv(os.path.join(path + 'data/cases.csv'), sep = ',')
        df_runners = pd.read_csv(os.path.join(path + 'data/runners.csv'), sep = ',')
        df_roles = pd.read_csv(os.path.join(path + 'data/roles.csv'), sep = ',')
        df_assistants = pd.read_csv(os.path.join(path + 'data/assistants.csv'), sep = ',')
        df_ftes = pd.read_csv(os.path.join(path + 'data/historical_ftes.csv'), sep = ',')

        ###FASE DE TRANSFORMACION
        
        support_groups = ['Implementacion de Servicios Estrategicos CedEx Soporte',
                    'Operaciones CedEx Implementacion de Servicios Estrategicos CedEx Soporte Enterdev',
                    'Operaciones CedEx Soporte en Campo UNISYS']

        resolved_states = ['Resuelto', 'Cerrado']

        df_cases = (df_cases.query("Grupo in @support_groups")
                        .query("Estado in @resolved_states")
                        .query("Resumen.str.startswith('RPA')")
                        .query("Sintoma.str.startswith('ACIS')"))

        df_cases['assistant_nickname'] = df_cases['Resumen'].str.split(' - ').str[0].str.upper()
        df_cases['Sintoma'] = df_cases['Sintoma'].str.replace('?', '', regex = False)
        df_cases['symptom_category'] = df_cases['Sintoma'].str.split(' - ').str[1]

        df_runners = df_runners.dropna(subset = ['machine'])
        df_runners['machine'] = df_runners['machine'].str.upper()
        df_runners = pd.merge(df_runners[['role_id', 'machine']], df_roles[['role_id', 'assistant_id']], on = 'role_id', how = 'inner')
        df_runners = df_runners.groupby(by = 'assistant_id').agg({'machine': 'nunique'}).reset_index()


        df_assistants = (df_assistants.query("assistant_type == 'RPA'")
                                    .query("assistant_state == 'Activo'"))

        df_assistants['assistant_nickname'] = df_assistants['assistant_nickname'].str.upper()
        df_assistants = pd.merge(df_assistants[['assistant_id', 'assistant_nickname', 'assistant_bia']], df_runners, on = 'assistant_id', how = 'inner')

        df_ftes = (df_ftes.query("anio == 2022")
                        .query("mes in (10, 11, 12)"))

        df_ftes = df_ftes.groupby(by = 'id_componente').agg({'ftes': 'mean'}).reset_index()

        df_assistants = pd.merge(df_assistants, df_ftes, left_on = 'assistant_id', right_on = 'id_componente', how = 'inner')

        df_cases = pd.merge(df_cases, df_assistants[['assistant_nickname', 'assistant_bia', 'machine', 'ftes']], on = 'assistant_nickname', how = 'inner')

        df_cases['Urgencia'].unique()

        df1 = pd.get_dummies(df_cases, columns = ['Urgencia'], drop_first = False)

        df_perc = df1.groupby('assistant_nickname').agg(
            perc_inmediata = (
                "Urgencia_Inmediata", 
                lambda x: round(sum(x)/len(x),3)),
            perc_puede_esperar = (
                "Urgencia_Puede Esperar", 
                lambda x: round(sum(x)/len(x),3)),
            perc_media = (
                "Urgencia_Media", 
                lambda x: round(sum(x)/len(x),3)),
            perc_alta= (
                "Urgencia_Alta", 
                lambda x: round(sum(x)/len(x),3)),
            perc_no_urg = (
                "Urgencia_No es urgente", 
                lambda x: round(sum(x)/len(x),3))
        ).reset_index()

        pd.DataFrame(df_perc)

        df_final = pd.merge(df1,df_perc,on='assistant_nickname', how='left')

        ### FASE DE CARGA
        existe_archivo = os.path.isfile(path + 'salidas/bd_consolidada.csv')
        if not existe_archivo:
            try:
                df_final.to_csv(path + 'salidas/bd_consolidada.csv')
            except:
                os.mkdir(path + 'salidas')
                df_final.to_csv(path + 'salidas/bd_consolidada.csv')

        else: 
            try:
                os.remove(path + 'salidas/bd_consolidada.csv')
                df_final.to_csv(path + 'salidas/bd_consolidada.csv')
            except:
                print('No es posible guardar el archivo')

        