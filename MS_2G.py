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

def processArchive(periodo):
    fields = [periodo,'Município','ANF','BSC/RNC','Station ID','RAN Node','Banda','Tecnologia','VOLUME_DADOS_DLUL_ALLOP 2G - Mbyte','TRAFEGO_VOZ_TIM 2G','ACC_DEN1','ACC_DEN2','ACC_DEN3','ACC_NUM1','ACC_NUM2','ACC_NUM3','ACC_GPRS_DEN (DL+UL)','ACC_GPRS_NUM (DL+UL)','TCH_DROP_BTS_DEN','TCH_DROP_BTS_NUM','SDCCH_DROP_DEM','SDCCH_DROP_NUM','DISP_COUNTER_TOTAL_DEN 2G (sem filtro OPER)','DISP_COUNTER_TOTAL_NUM 2G (sem filtro OPER)']
    fields2 = [periodo,'Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Banda','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','TRAFEGO_VOZ_TIM','ACC_DEN1','ACC_DEN2','ACC_DEN3','ACC_NUM1','ACC_NUM2','ACC_NUM3','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM']
    
    pathImport = '/import/2G'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/MS/'+'Dados'+'2G.csv')
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
    #frameSI[periodo] = frameSI[periodo].str[:3]
    
    frameSI.insert(len(frameSI.columns),'THROU_USER_DEN',0)
    frameSI.insert(len(frameSI.columns),'THROU_USER_NUM',0)
    frameSI.insert(len(frameSI.columns),'USERS',0)
    frameSI.insert(len(frameSI.columns),'INTER1',0)
    frameSI.insert(len(frameSI.columns),'INTER2',0)
    #USERS

    frameSI['TRAFEGO_VOZ_TIM'] = frameSI['TRAFEGO_VOZ_TIM'].apply(lambda x: x.replace(".","").replace(",","."))
    frameSI['TRAFEGO_VOZ_TIM'] = frameSI['TRAFEGO_VOZ_TIM'].astype(float)
    #frameSI['TRAFEGO_VOZ_TIM'] = frameSI['TRAFEGO_VOZ_TIM'].astype(int)

    frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'] = frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].apply(lambda x: x.replace(".","").replace(",","."))
    frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'] = frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].astype(float)/1024 #converter para GB
    
    #frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'] = frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].astype(int)



    print(frameSI[['TRAFEGO_VOZ_TIM','VOLUME_DADOS_DLUL_ALLOP(Mbyte)']])

    '''
    frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'] = [x.replace(',', '.') for x in frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)']]
    frameSI['TRAFEGO_VOZ_TIM'] = [x.replace(',', '.') for x in frameSI['TRAFEGO_VOZ_TIM']]
    '''

    lista1 = ['ACC_DEN1','ACC_DEN2','ACC_DEN3','ACC_NUM1','ACC_NUM2','ACC_NUM3','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM']
    for i in lista1:
      frameSI[i] = [x.replace('.', '') for x in frameSI[i]]
      frameSI[i] = [x.replace('(', '') for x in frameSI[i]]
      frameSI[i] = [x.replace(')', '') for x in frameSI[i]]


    
    frameSI['ACV_DEN'] = frameSI['ACC_DEN1'].astype(np.int64) * frameSI['ACC_DEN2'].astype(np.int64)  * frameSI['ACC_DEN3'].astype(np.int64) 
    frameSI['ACV_NUM'] = frameSI['ACC_NUM1'].astype(np.int64) * frameSI['ACC_NUM2'].astype(np.int64)  * frameSI['ACC_NUM3'].astype(np.int64) 
    
    frameSI['Peso_ACD'] = frameSI['ACD_DEN'].astype(np.int64) - frameSI['ACD_NUM'].astype(np.int64)
    frameSI['Peso_DROP_VOZ'] = frameSI['DROP_VOZ_DEN'].astype(np.int64) - frameSI['DROP_VOZ_NUM'].astype(np.int64)
    frameSI['Peso_DROP_DADOS'] = frameSI['DROP_DADOS_DEN'].astype(np.int64) - frameSI['DROP_DADOS_NUM'].astype(np.int64)
    frameSI['Peso_DISP'] = frameSI['DISP_DEN'].astype(np.int64) - frameSI['DISP_NUM'].astype(np.int64)
    frameSI['Peso_ACV'] = (frameSI['ACC_DEN1'].astype(np.int64) + frameSI['ACC_DEN2'].astype(np.int64) + frameSI['ACC_DEN3'].astype(np.int64)) - (frameSI['ACC_NUM1'].astype(np.int64) + frameSI['ACC_NUM2'].astype(np.int64)  + frameSI['ACC_NUM3'].astype(np.int64) )
    
    
    
    frameSI = frameSI.drop(['ACC_NUM1', 'ACC_NUM2','ACC_NUM3','ACC_DEN1','ACC_DEN2','ACC_DEN3'], axis=1)
 
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO