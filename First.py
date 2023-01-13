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

def processArchive(Agregacao,periodo):
    fields = [periodo,Agregacao,'Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Gbyte)','TRAFEGO_VOZ_TIM','USERS','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','INTER1','INTER2']
    fields2 = [periodo,Agregacao,'Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Gbyte)','TRAFEGO_VOZ_TIM','USERS','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','INTER1','INTER2']
    
    pathImport = '/export/MS_ALL'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/MUNICIPIO/'+archiveName+'.csv')
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


    frameSI = frameSI.astype(str)
    lista1 = frameSI.columns
    for i in lista1:
      frameSI[i] = [x.replace(',', '.') for x in frameSI[i]]

    
    #frameSI = frameSI.loc[frameSI['Black_List'].isna()]


    ref_List = [Agregacao]
    tec_List = ['2G','3G','4G']
    col_Dia = frameSI[periodo].unique()
    header = frameSI.columns.values.tolist()
    for ref in ref_List:
      col_ref_List = frameSI[ref].unique()
      for tec in tec_List:
        index1 = 0
        for ref_Index in col_ref_List:
          frameSI2 = frameSI.loc[(frameSI[ref] == ref_Index) & (frameSI['Tecnologia'] == tec)]
          for dia in col_Dia:
            frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_ACD_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'ACD_NUM'].astype(float).sum() / frameSI2.loc[frameSI2[periodo]== dia,'ACD_DEN'].astype(float).sum()
            frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_DROP_VOZ_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'DROP_VOZ_NUM'].astype(float).sum() / frameSI2.loc[frameSI2[periodo]== dia,'DROP_VOZ_DEN'].astype(float).sum()
            frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_DROP_DADOS_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'DROP_DADOS_NUM'].astype(float).sum() / frameSI2.loc[frameSI2[periodo]== dia,'DROP_DADOS_DEN'].astype(float).sum()
            frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_DISP_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'DISP_NUM'].astype(float).sum() / frameSI2.loc[frameSI2[periodo]== dia,'DISP_DEN'].astype(float).sum()
            frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_THROU_USER_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'THROU_USER_NUM'].astype(float).sum() / frameSI2.loc[frameSI2[periodo]== dia,'THROU_USER_DEN'].astype(float).sum()
            frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_ACV_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'ACV_NUM'].astype(float).sum() / frameSI2.loc[frameSI2[periodo]== dia,'ACV_DEN'].astype(float).sum()
            frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'VOLUME_DADOS_DLUL_ALLOP(Gbyte)'].astype(float).sum()
            frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_TRAFEGO_VOZ_TIM_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'TRAFEGO_VOZ_TIM'].astype(float).sum()
            #frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_USERS_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'USERS'].astype(float).sum()
            #frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_USERS_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'USERS'].astype(float).median() # median mediana de uma lista
            frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_USERS_'+ref]] = frameSI2.loc[(frameSI2[periodo]== dia),'USERS'].astype(float).mean() # media simples usada no MS
            frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_INTER1_'+ref]] = frameSI2.loc[(frameSI2[periodo]== dia) & (frameSI2['INTER1'].astype(float) != 0),'INTER1'].astype(float).max() # media simples usada no MS .mean()
            frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_INTER2_'+ref]] = frameSI2.loc[(frameSI2[periodo]== dia) & (frameSI2['INTER2'].astype(float) != 0),'INTER2'].astype(float).max() # media simples usada no MS
            
            #frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_CRITICO_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'CRITICO'].astype(float).sum()
            #frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_ALERTA_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'ALERTA'].astype(float).sum()
            #frameSI2.loc[(frameSI2[periodo] == dia),[tec+'_BOM_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'BOM'].astype(float).sum()
            #frameSI2.loc[(frameSI2[periodo] == dia) & (frameSI2[tec+'_ACD_'+ref] < 0.98),['_ACD_Impact']] = 1
          
              
          

          #frameSI2[tec+'_OCUPACAO_'+ref] = frameSI2[tec+'_CRITICO_'+ref].astype(float) / (frameSI2[tec+'_CRITICO_'+ref].astype(float) + frameSI2[tec+'_ALERTA_'+ref].astype(float) + frameSI2[tec+'_BOM_'+ref].astype(float))
        


          frameSI2 = frameSI2.astype(str)    
          dropList = ['VOLUME_DADOS_DLUL_ALLOP(Gbyte)','TRAFEGO_VOZ_TIM','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','USERS']#,'CRITICO','ALERTA','BOM'
          frameSI2 = frameSI2.drop(dropList, axis=1)
          frameSI2 = frameSI2.drop_duplicates()
          

          lista1 = frameSI2.columns
          for i in lista1:
            frameSI2[i] = [x.replace('.', ',') for x in frameSI2[i]]
          
          if index1 == 0:
            frame_ALL = frameSI2.copy()
            index1 = 1
          else:
            frame_ALL = frame_ALL.append(frameSI2,ignore_index = True)
          #print(len(frame_ALL.index))  
        #csv_path = os.path.join(script_dir, 'export/'+ ref +'/'+tec+'/'+ tec + '_'+ ref +'.csv')
        #csv_path = os.path.join(script_dir, 'export/'+ ref +'/'+tec+'/'+ DateCreation + '_' + tec + '_'+ ref +'.csv')
        csv_path = os.path.join(script_dir, 'export/'+ ref +'_W'+'/'+tec+'/'+ tec + '_'+ ref +'.csv')
        frame_ALL = frame_ALL.loc[frame_ALL[Agregacao] != 'nan']
        frame_ALL.to_csv(csv_path,index=False,header=True,sep=';')
        print(tec,'_',ref,' Saved!')





