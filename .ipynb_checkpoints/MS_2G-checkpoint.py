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
    fields = ['Dia','Semana do Ano','Município','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Banda','Célula','Tecnologia','VOLUME_DADOS_DLUL_ALLOP 2G - Mbyte','TRAFEGO_VOZ_TIM 2G','ACC_DEN1','ACC_DEN2','ACC_DEN3','ACC_NUM1','ACC_NUM2','ACC_NUM3','ACC_GPRS_DEN (DL+UL)','ACC_GPRS_NUM (DL+UL)','TCH_DROP_BTS_DEN','TCH_DROP_BTS_NUM','SDCCH_DROP_DEM','SDCCH_DROP_NUM','DISP_COUNTER_TOTAL_DEN 2G (sem filtro OPER)','DISP_COUNTER_TOTAL_NUM 2G (sem filtro OPER)']
    fields2 = ['Dia','Semana','Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Banda','Celula','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','TRAFEGO_VOZ_TIM','ACC_DEN1','ACC_DEN2','ACC_DEN3','ACC_NUM1','ACC_NUM2','ACC_NUM3','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM']
    
    pathImport = '/import/2G'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/MS/'+archiveName+'.csv')
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
        df = df.fillna(0)
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2
    frameSI = frameSI.astype(str)
    frameSI['Semana'] = frameSI['Semana'].str[:3]
    frameSI['CellSector'] = frameSI['Celula'].str[-1:]
    frameSI['CELL_Fisico'] = frameSI['CellSector'].map({'0':'0', '1':'1', '2':'2', '3':'3', 'A':'1', 'B':'2', 'C':'3', 'D':'4', 'E':'1', 'F':'2', 'G':'3', 'H':'4', 'I':'1', 'J':'2', 'K':'3', 'L':'4', 'M':'1', 'N':'2', 'P':'3', 'Q':'1', 'R':'2', 'S':'3', 'T':'4', 'U':'2', 'V':'3', 'W':'0', 'X':'1', 'Y':'2', 'Z':'3'})
    
    frameSI.insert(len(frameSI.columns),'THROU_USER_DEN',0)
    frameSI.insert(len(frameSI.columns),'THROU_USER_NUM',0)

    frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'] = [x.replace(',', '.') for x in frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)']]
    frameSI['TRAFEGO_VOZ_TIM'] = [x.replace(',', '.') for x in frameSI['TRAFEGO_VOZ_TIM']]


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
    
    
    
    frameSI = frameSI.drop(['CellSector','ACC_NUM1', 'ACC_NUM2','ACC_NUM3','ACC_DEN1','ACC_DEN2','ACC_DEN3'], axis=1)
    frameSI = frameSI.astype(str)
    lista1 = ['VOLUME_DADOS_DLUL_ALLOP(Mbyte)','TRAFEGO_VOZ_TIM','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV']
    for i in lista1:
      frameSI[i] = [x.replace('.', ',') for x in frameSI[i]]
    
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO