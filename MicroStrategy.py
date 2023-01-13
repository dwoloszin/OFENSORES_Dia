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
    fields = ['BTS/NodeB/ENodeB','Célula','Dia','Data Primeiro Tráfego','Data Último Tráfego','Último Tráfego Voz Registrado','Último Volume Dados Registrado','Tráfego de Voz','Volume de Dados']
    fields2 = ['BTS/NodeB/ENodeB','CELL','Dia','Data Primeiro Tráfego','Data Último Tráfego','Último Tráfego Voz Registrado','Último Volume Dados Registrado','Tráfego de Voz','Volume de Dados']
    
    
    pathImport = '/import/2G'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/2G/'+archiveName+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    df = pd.DataFrame()
    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None,skiprows=2, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv])
        df = pd.concat([chunk for chunk in iter_csv])
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2 




    '''
    frameSI['Último Tráfego Voz Registrado'] = frameSI['Último Tráfego Voz Registrado'].map(lambda x: x.lstrip(' ()').rstrip(' ()'))
    frameSI['Último Tráfego Voz Registrado']  = frameSI['Último Tráfego Voz Registrado'].str.replace(',','.')

    frameSI['Tráfego de Voz'] = frameSI['Tráfego de Voz'].map(lambda x: x.lstrip(' ()').rstrip(' ()'))
    frameSI['Tráfego de Voz']  = frameSI['Tráfego de Voz'].str.replace(',','.')

    frameSI['Último Volume Dados Registrado'] = frameSI['Último Volume Dados Registrado'].map(lambda x: x.lstrip(' ()').rstrip(' ()'))
    frameSI['Último Volume Dados Registrado']  = frameSI['Último Volume Dados Registrado'].str.replace('.','')

    frameSI['Volume de Dados'] = frameSI['Volume de Dados'].map(lambda x: x.lstrip(' ()').rstrip(' ()'))
    frameSI['Volume de Dados']  = frameSI['Volume de Dados'].str.replace('.','')
    

    frameSI['Data Primeiro Tráfego1'] = pd.to_datetime(frameSI['Data Primeiro Tráfego'], format="%d/%m/%Y")
    frameSI['Data Último Tráfego1'] = pd.to_datetime(frameSI['Data Último Tráfego'], format="%d/%m/%Y")
    frameSI['ActiveCellTime(days)'] = (frameSI['Data Último Tráfego1'] - frameSI['Data Primeiro Tráfego1']).dt.days

    frameSI = RemoveDuplcates.processarchive(frameSI,'CELL','Data Último Tráfego1')
    




    frameSI.insert(len(frameSI.columns),'STATUS','SEM TRAFEGO')
    
    frameSI.loc[(frameSI['Último Tráfego Voz Registrado'].astype(float)> 0) | 
                                    (frameSI['Último Volume Dados Registrado'].astype(float)> 0)|
                                    (frameSI['Tráfego de Voz'].astype(float)> 0)|
                                    (frameSI['Volume de Dados'].astype(float)> 0), ['STATUS']] = 'ACTIVE'

    frameSI['Dia1'] = pd.to_datetime(frameSI['Dia'], format="%d/%m/%Y")
    frameSI.loc[((frameSI['Data Último Tráfego1'] - frameSI['Dia1']).dt.days) < 0,['STATUS']] = 'PAROU DE REPORTAR'


    #frameSI['STATUS'] = frameSI.loc[frameSI['']]
    print(frameSI)
    '''
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO