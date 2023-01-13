import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import RemoveDuplcates
import warnings
warnings.simplefilter("ignore")# TIM_VOLUME_DADOS_DLUL_ALLOP (KB)

def processArchive(periodo):
    fields = [periodo,'Municipality','ANF','NodeB','Cell',' TIM_VOLUME_DADOS_DLUL_ALLOP (KB)',' TIM_TRAFEGO_VOZ_ALLOP (E)',' TIM_SMP5_PMQ_DEN1 (Unid)',' TIM_SMP5_PMQ_DEN2 (Unid)',' TIM_SMP5_PMQ_NUM1 (Unid)',' TIM_SMP5_PMQ_NUM2 (Unid)',' TIM_SMP8_PMQ_DEN (Unid)',' TIM_SMP8_PMQ_NUM (Unid)',' TIM_SMP7_PMQ_DEN (Unid)',' TIM_SMP7_PMQ_NUM (Unid)',' TIM_SMP9_PMQ_DEN (Unid)',' TIM_SMP9_PMQ_NUM (Unid)',' TIM_DISP_COUNTER_TOTAL_DEN (s)',' TIM_DISP_COUNTER_TOTAL_NUM (s)',' TIM_THROU_USER_HSDPA_DL_DEN (s)',' TIM_THROU_USER_HSDPA_DL_NUM (Kb)',' TIM_RTWP_MEAN (Unid)']
    fields2 = [periodo,'Municipio','ANF','BTS/NodeB/ENodeB','Cell','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','TRAFEGO_VOZ_TIM','ACC_DEN1','ACC_DEN2','ACC_NUM1','ACC_NUM2','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','INTER1']
    
    pathImport = '/import/3G/ALTAIA'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[11:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/MS/'+'Dados'+'3G.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    df = pd.DataFrame()
    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None,skiprows=0, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str, sep = ',',iterator=True, chunksize=10000, usecols = fields)
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv])
        df = pd.concat([chunk for chunk in iter_csv])
        df = df.fillna(0)
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2
    frameSI = frameSI.astype(str)
    #frameSI[periodo] = frameSI[periodo].str[:3]

    frameSI.insert(len(frameSI.columns),'USERS',0)
    #USERS

    frameSI.insert(3,'BSC/RNC',0)
    frameSI.insert(4,'Station ID',0)
    frameSI.insert(6,'Banda',0)
    frameSI.insert(8,'Tecnologia','3G')
    frameSI.insert(len(frameSI.columns),'INTER2',0)

    #Tecnologia

    #Limpar 3G 900
    frameSI = frameSI.loc[~frameSI['Cell'].str[-1:].isin(['E','F','G'])]





    frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'] = frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].astype(float)/(1024*1024) #converter para GB



    
    frameSI['ACV_DEN'] = frameSI['ACC_DEN1'].astype(float) * frameSI['ACC_DEN2'].astype(float)
    frameSI['ACV_NUM'] = frameSI['ACC_NUM1'].astype(float) * frameSI['ACC_NUM2'].astype(float)
    
    frameSI['Peso_ACD'] = frameSI['ACD_DEN'].astype(float) - frameSI['ACD_NUM'].astype(float)
    frameSI['Peso_DROP_VOZ'] = frameSI['DROP_VOZ_DEN'].astype(float) - frameSI['DROP_VOZ_NUM'].astype(float)
    frameSI['Peso_DROP_DADOS'] = frameSI['DROP_DADOS_DEN'].astype(float) - frameSI['DROP_DADOS_NUM'].astype(float)
    frameSI['Peso_DISP'] = frameSI['DISP_DEN'].astype(float) - frameSI['DISP_NUM'].astype(float)
    frameSI['Peso_ACV'] = (frameSI['ACC_DEN1'].astype(float) + frameSI['ACC_DEN2'].astype(float)) - (frameSI['ACC_NUM1'].astype(float) + frameSI['ACC_NUM2'].astype(float))
    
    
    
    frameSI = frameSI.drop(['ACC_NUM1', 'ACC_NUM2','ACC_DEN1','ACC_DEN2'], axis=1)
    coll_list = ['Data','Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Cell','Banda','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','TRAFEGO_VOZ_TIM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','USERS','ACV_DEN','ACV_NUM','ACD_DEN','ACD_NUM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV','INTER1','INTER2']
    frameSI = frameSI.loc[:, coll_list]
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO