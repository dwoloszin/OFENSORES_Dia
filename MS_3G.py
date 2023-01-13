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
    fields = [periodo,'Município','ANF','BSC/RNC','Station ID','RAN Node','Banda','Tecnologia','VOLUME_DADOS_DLUL_ALLOP 3G - Mbyte','TRAFEGO_VOZ_TIM 3G','ACV_UT_DEN1','ACV_UT_DEN2','ACV_UT_NUM1','ACV_UT_NUM2','ACD_UT_DEN1','ACD_UT_DEN2','ACD_UT_NUM1','ACD_UT_NUM2','DROP_PS_DEN','DROP_PS_NUM','DISP_COUNTER_TOTAL_DEN 3G (sem filtro OPER)','DISP_COUNTER_TOTAL_NUM 3G (sem filtro OPER)','THROU_USER_HSDPA_DL_DEN','THROU_USER_HSDPA_DL_NUM','DROP_CS_AMR_DEN','DROP_CS_AMR_NUM']
    fields2 = [periodo,'Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Banda','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','TRAFEGO_VOZ_TIM','ACV_UT_DEN1','ACV_UT_DEN2','ACV_UT_NUM1','ACV_UT_NUM2','ACD_UT_DEN1','ACD_UT_DEN2','ACD_UT_NUM1','ACD_UT_NUM2','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM']
    
    pathImport = '/import/3G'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
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
        iter_csv = pd.read_csv(filename, index_col=None,skiprows=2, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
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

    frameSI['TRAFEGO_VOZ_TIM'] = frameSI['TRAFEGO_VOZ_TIM'].apply(lambda x: x.replace(".","").replace(",","."))
    frameSI['TRAFEGO_VOZ_TIM'] = frameSI['TRAFEGO_VOZ_TIM'].astype(float)
    #frameSI['TRAFEGO_VOZ_TIM'] = frameSI['TRAFEGO_VOZ_TIM'].astype(int)

    frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'] = frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].apply(lambda x: x.replace(".","").replace(",","."))
    frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'] = frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].astype(float)/1024 # Converter para GB

    #frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'] = frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].astype(int)

    lista1 = ['ACV_UT_DEN1','ACV_UT_DEN2','ACV_UT_NUM1','ACV_UT_NUM2','ACD_UT_DEN1','ACD_UT_DEN2','ACD_UT_NUM1','ACD_UT_NUM2','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM']
    for i in lista1:
      frameSI[i] = [x.replace('.', '') for x in frameSI[i]]
      frameSI[i] = [x.replace('(', '') for x in frameSI[i]]
      frameSI[i] = [x.replace(')', '') for x in frameSI[i]]



    frameSI['ACV_DEN'] = frameSI['ACV_UT_DEN1'].astype(np.int64) * frameSI['ACV_UT_DEN2'].astype(np.int64) 
    frameSI['ACV_NUM'] = frameSI['ACV_UT_NUM1'].astype(np.int64) * frameSI['ACV_UT_NUM2'].astype(np.int64) 
    
    frameSI['ACD_DEN'] = frameSI['ACD_UT_DEN1'].astype(np.int64) * frameSI['ACD_UT_DEN2'].astype(np.int64) 
    frameSI['ACD_NUM'] = frameSI['ACD_UT_NUM1'].astype(np.int64) * frameSI['ACD_UT_NUM2'].astype(np.int64) 


    frameSI['Peso_ACD'] = (frameSI['ACD_UT_DEN1'].astype(np.int64) + frameSI['ACD_UT_DEN2'].astype(np.int64)) - (frameSI['ACD_UT_NUM1'].astype(np.int64) + frameSI['ACD_UT_NUM2'].astype(np.int64))
    frameSI['Peso_DROP_VOZ'] = frameSI['DROP_VOZ_DEN'].astype(np.int64) - frameSI['DROP_VOZ_NUM'].astype(np.int64)
    frameSI['Peso_DROP_DADOS'] = frameSI['DROP_DADOS_DEN'].astype(np.int64) - frameSI['DROP_DADOS_NUM'].astype(np.int64)
    frameSI['Peso_DISP'] = frameSI['DISP_DEN'].astype(np.int64) - frameSI['DISP_NUM'].astype(np.int64)
    frameSI['Peso_ACV'] = (frameSI['ACV_UT_DEN1'].astype(np.int64) + frameSI['ACV_UT_DEN2'].astype(np.int64)) - (frameSI['ACV_UT_NUM1'].astype(np.int64) + frameSI['ACV_UT_NUM2'].astype(np.int64))
    

    frameSI = frameSI.drop(['ACV_UT_DEN1', 'ACV_UT_DEN2','ACV_UT_NUM1','ACV_UT_NUM2','ACD_UT_DEN1', 'ACD_UT_DEN2','ACD_UT_NUM1','ACD_UT_NUM2'], axis=1)

    

    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO