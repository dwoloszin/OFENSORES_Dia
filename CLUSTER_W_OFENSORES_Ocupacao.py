import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import RemoveDuplcates
import warnings
import datetime
warnings.simplefilter("ignore")

def processArchive():
    fields = ['Semana','Municipio','ANF','Station ID','BTS/NodeB/ENodeB','Celula','NOME_DO_SU','CRITICO']
    fields2 = ['Semana','Municipio','ANF','Station ID','BTS/NodeB/ENodeB','Celula','NOME_DO_SU','CRITICO']
    
    pathImport = '/export/MS_ALL'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    df = pd.DataFrame()
    DateCreation = datetime.datetime.fromtimestamp(os.path.getmtime(all_filesSI[0])).strftime("%Y%m%d")
    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None,skiprows=0, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv])
        df = pd.concat([chunk for chunk in iter_csv])
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2


    #frameSI = frameSI.astype(str)
    frameSI = frameSI.loc[(~frameSI['NOME_DO_SU'].isna()) & (frameSI['CRITICO'].astype(float) > 0)]
    Site_List = frameSI['BTS/NodeB/ENodeB'].unique()
    Week_List = frameSI['Semana'].unique()

    
    for site in Site_List:
      frameSI.loc[(frameSI['BTS/NodeB/ENodeB'] == site),['_Peso_ALL']] = frameSI.loc[frameSI['BTS/NodeB/ENodeB']== site,'CRITICO'].astype(int).sum()
      for week in Week_List:
        frameSI.loc[(frameSI['BTS/NodeB/ENodeB'] == site) & (frameSI['Semana'] == week),['_Peso_Week']] = frameSI.loc[(frameSI['BTS/NodeB/ENodeB'] == site) & (frameSI['Semana'] == week),'CRITICO'].astype(int).sum()

    frameSI = frameSI.sort_values(['Semana','_Peso_Week','_Peso_ALL'], ascending = [False,False,False])
     
    csv_path = os.path.join(script_dir, 'export/NOME_DO_SU_W_Ofensor/'+ 'Ofensores.csv')
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

    



