import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import RemoveDuplcates
import warnings
warnings.simplefilter("ignore")

def processArchive():
    fields = ['Semana Ocupação','Município','ANF','Station ID','BTS/NodeB/ENodeB','Banda','Célula','Classificação','Ind Canal DL 4G','Fator_Plan_LT','Vol_Total_Dl_Allop_LT','Throu_Pdcp_Cell_Dl_LT','Throu_User _Pdcp_Dl_LT','Act_Ue_Data_Dl_LT','Users_Rrc_Conn_Mean_LT','Cqi_Mean_LT','Prb_Util_Mean_Dl_LT','Tti_Util_LT','Util_LT','Vol_Total_DlUl_Allop_LT','Vol_Total_DlUl_Tim_LT']
    fields2 = ['Semana','Municipio','ANF','Station ID','BTS/NodeB/ENodeB','Banda','Celula','Classificacao','Ind Canal DL 4G','Fator_Plan_LT','Vol_Total_Dl_Allop_LT','Throu_Pdcp_Cell_Dl_LT','Throu_User _Pdcp_Dl_LT','Act_Ue_Data_Dl_LT','Users_Rrc_Conn_Mean_LT','Cqi_Mean_LT','Prb_Util_Mean_Dl_LT','Tti_Util_LT','Util_LT','Vol_Total_DlUl_Allop_LT','Vol_Total_DlUl_Tim_LT']
    
    pathImport = '/import/OCUPACAO'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/MS_OCUPACAO/'+archiveName+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    df = pd.DataFrame()
    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None,skiprows=2, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields)
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv])
        df = pd.concat([chunk for chunk in iter_csv])
        df = df.fillna(0)
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2
    frameSI = frameSI.astype(str)
    frameSI['Semana'] = 'W' + frameSI['Semana'].str[-2:]
    frameSI.insert(len(frameSI.columns),'CRITICO',0)
    frameSI.insert(len(frameSI.columns),'ALERTA',0)
    frameSI.insert(len(frameSI.columns),'BOM',0)
    frameSI.loc[frameSI['Classificacao'] == 'Crítico',['CRITICO']] = 1
    frameSI.loc[frameSI['Classificacao'] == 'Alerta',['ALERTA']] = 1
    frameSI.loc[frameSI['Classificacao'] == 'Bom',['BOM']] = 1
    
    
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO