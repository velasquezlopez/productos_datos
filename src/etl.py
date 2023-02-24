import pandas as pd
import os


class ExtractTransformLoad():

    def ETL(self):
    ###FASE DE EXTRACCION
        path = self.get_folderPath()
        df_cases = pd.read_csv(os.path.join(path + 'data/cases.csv'), sep = ',')
        df_runners = pd.read_csv(os.path.join(path + 'data/runners.csv'), sep = ',')
        df_roles = pd.read_csv(os.path.join(path + 'data/roles.csv'), sep = ',')
        df_assistants = pd.read_csv(os.path.join(path + 'data/assistants.csv'), sep = ',')
        df_ftes = pd.read_csv(os.path.join(path + 'data/historical_ftes.csv'), sep = ',')
        df_joined = self.unir_df(df_cases,df_runners,df_assistants,df_roles,df_ftes)
        df_final = self.preProcesamiento(df_joined)
        self.cargar(df_final)
    
    def get_folderPath(self):
    #Obtener la ruta de la carpeta para leer archivos
        self.path = os.path.dirname(os.path.abspath(__file__))
        self.path = os.path.join(self.path + '/')
        return self.path 
        
        
    def unir_df(self,cases,runners,assistants,roles,ftes):
        support_groups = ['Implementacion de Servicios Estrategicos CedEx Soporte',
                    'Operaciones CedEx Implementacion de Servicios Estrategicos CedEx Soporte Enterdev',
                    'Operaciones CedEx Soporte en Campo UNISYS']

        resolved_states = ['Resuelto', 'Cerrado']

        cases = (cases.query("Grupo in @support_groups")
                        .query("Estado in @resolved_states")
                        .query("Resumen.str.startswith('RPA')")
                        .query("Sintoma.str.startswith('ACIS')"))

        cases['assistant_nickname'] = cases['Resumen'].str.split(' - ').str[0].str.upper()
        cases['Sintoma'] = cases['Sintoma'].str.replace('?', '', regex = False)
        cases['symptom_category'] = cases['Sintoma'].str.split(' - ').str[1]

        runners = runners.dropna(subset = ['machine'])
        runners['machine'] = runners['machine'].str.upper()
        runners = pd.merge(runners[['role_id', 'machine']], roles[['role_id', 'assistant_id']], on = 'role_id', how = 'inner')
        runners = runners.groupby(by = 'assistant_id').agg({'machine': 'nunique'}).reset_index()


        assistants = (assistants.query("assistant_type == 'RPA'")
                                    .query("assistant_state == 'Activo'"))

        assistants['assistant_nickname'] = assistants['assistant_nickname'].str.upper()
        assistants = pd.merge(assistants[['assistant_id', 'assistant_nickname', 'assistant_bia']], runners, on = 'assistant_id', how = 'inner')

        ftes = (ftes.query("anio == 2022")
                        .query("mes in (10, 11, 12)"))

        ftes = ftes.groupby(by = 'id_componente').agg({'ftes': 'mean'}).reset_index()

        assistants = pd.merge(assistants, ftes, left_on = 'assistant_id', right_on = 'id_componente', how = 'inner')

        fin = pd.merge(cases, assistants[['assistant_nickname', 'assistant_bia', 'machine', 'ftes']], on = 'assistant_nickname', how = 'inner')

        return fin 
    
    def preProcesamiento(self, df):
        df['Urgencia'].unique()

        df1 = pd.get_dummies(df, columns = ['Urgencia'], drop_first = False)

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

        return df_final
    
    def cargar(self, df):

        ### FASE DE CARGA
        path = self.get_folderPath()
        existe_archivo = os.path.isfile(path + 'salidas/bd_consolidada.csv')
        if not existe_archivo:
            try:
                df.to_csv(path + 'salidas/bd_consolidada.csv')
                print('Archivo guardado exitosamente')
            except:
                os.mkdir(path + 'salidas')
                df.to_csv(path + 'salidas/bd_consolidada.csv')
                print('Archivo guardado exitosamente')

        else: 
            try:
                os.remove(path + 'salidas/bd_consolidada.csv')
                df.to_csv(path + 'salidas/bd_consolidada.csv')
                print('Archivo guardado exitosamente')
            except:
                print('No es posible guardar el archivo')

        