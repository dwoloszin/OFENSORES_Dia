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
    fields = ['Dia','Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Banda','Celula','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV']
    fields2 = ['Dia','Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Banda','Celula','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV']
    
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
    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None,skiprows=0, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv])
        df = pd.concat([chunk for chunk in iter_csv])
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2
    frameSI.insert(len(frameSI.columns),'Dia1',pd.to_datetime(frameSI['Dia'].astype(str), format="%d/%m/%Y"))
    frameSI = frameSI.sort_values(['Dia1'], ascending = [True])

    frameSI = frameSI.astype(str)

    forOne = ['Municipio']
    forTwo = ['4G']

    for i in forOne:
      for j in forTwo:
        ref = i
        tec = j
        col_Mun = frameSI[ref].unique()
        col_Dia = frameSI['Dia'].unique()
        frame_ALL = pd.DataFrame()
        for mun in col_Mun:
          frameSI2 = frameSI.loc[(frameSI[ref] == mun) & (frameSI['Tecnologia'] == tec)]
          for dia in col_Dia:
            frameSI2.loc[(frameSI2['Dia'] == dia),[tec+'_ACD_'+ref]] = frameSI2.loc[frameSI2['Dia']== dia,'ACD_NUM'].astype(float).sum() / frameSI2.loc[frameSI2['Dia']== dia,'ACD_DEN'].astype(float).sum()
            frameSI2.loc[(frameSI2['Dia'] == dia),[tec+'_DROP_VOZ_'+ref]] = frameSI2.loc[frameSI2['Dia']== dia,'DROP_VOZ_NUM'].astype(float).sum() / frameSI2.loc[frameSI2['Dia']== dia,'DROP_VOZ_DEN'].astype(float).sum()
            frameSI2.loc[(frameSI2['Dia'] == dia),[tec+'_DROP_DADOS_'+ref]] = frameSI2.loc[frameSI2['Dia']== dia,'DROP_DADOS_NUM'].astype(float).sum() / frameSI2.loc[frameSI2['Dia']== dia,'DROP_DADOS_DEN'].astype(float).sum()
            frameSI2.loc[(frameSI2['Dia'] == dia),[tec+'_DISP_'+ref]] = frameSI2.loc[frameSI2['Dia']== dia,'DISP_NUM'].astype(float).sum() / frameSI2.loc[frameSI2['Dia']== dia,'DISP_DEN'].astype(float).sum()
            frameSI2.loc[(frameSI2['Dia'] == dia),[tec+'_THROU_USER_'+ref]] = frameSI2.loc[frameSI2['Dia']== dia,'THROU_USER_NUM'].astype(float).sum() / frameSI2.loc[frameSI2['Dia']== dia,'THROU_USER_DEN'].astype(float).sum()
            frameSI2.loc[(frameSI2['Dia'] == dia),[tec+'_ACV_'+ref]] = frameSI2.loc[frameSI2['Dia']== dia,'ACV_NUM'].astype(float).sum() / frameSI2.loc[frameSI2['Dia']== dia,'ACV_DEN'].astype(float).sum()
            frameSI2.loc[(frameSI2['Dia'] == dia),[tec+'_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_'+ref]] = frameSI2.loc[frameSI2['Dia']== dia,'VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].astype(float).sum()
          
          frameSI2 = frameSI2.astype(str)    
          dropList = ['Dia1','Station ID','BTS/NodeB/ENodeB','Banda','Celula','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV']
          frameSI2 = frameSI2.drop(dropList, axis=1)
          frameSI2 = frameSI2.drop_duplicates()

          lista1 = frameSI2.columns
          for i in lista1:
            frameSI2[i] = [x.replace('.', ',') for x in frameSI2[i]]

          frame_ALL = frame_ALL.append(frameSI2)
        csv_path = os.path.join(script_dir, 'export/MUNICIPIO/'+tec+'/'+ tec + '_'+ mun +'.csv')
        frame_ALL.to_csv(csv_path,index=False,header=True,sep=';')









    '''
    col_Mun = frameSI['Municipio'].unique()
    col_Dia = frameSI['Dia'].unique()

    for mun in col_Mun:
      frameSI2 = frameSI.loc[(frameSI['Municipio'] == mun) & (frameSI['Tecnologia'] == '4G')]
      for dia in col_Dia:
        frameSI2.loc[(frameSI2['Dia'] == dia),['4G_ACD_Muni']] = frameSI2.loc[frameSI2['Dia']== dia,'ACD_NUM'].astype(float).sum() / frameSI2.loc[frameSI2['Dia']== dia,'ACD_DEN'].astype(float).sum()
        frameSI2.loc[(frameSI2['Dia'] == dia),['4G_DROP_VOZ_Muni']] = frameSI2.loc[frameSI2['Dia']== dia,'DROP_VOZ_NUM'].astype(float).sum() / frameSI2.loc[frameSI2['Dia']== dia,'DROP_VOZ_DEN'].astype(float).sum()
        frameSI2.loc[(frameSI2['Dia'] == dia),['4G_DROP_DADOS_Muni']] = frameSI2.loc[frameSI2['Dia']== dia,'DROP_DADOS_NUM'].astype(float).sum() / frameSI2.loc[frameSI2['Dia']== dia,'DROP_DADOS_DEN'].astype(float).sum()
        frameSI2.loc[(frameSI2['Dia'] == dia),['4G_DISP_Muni']] = frameSI2.loc[frameSI2['Dia']== dia,'DISP_NUM'].astype(float).sum() / frameSI2.loc[frameSI2['Dia']== dia,'DISP_DEN'].astype(float).sum()
        frameSI2.loc[(frameSI2['Dia'] == dia),['4G_THROU_USER_Muni']] = frameSI2.loc[frameSI2['Dia']== dia,'THROU_USER_NUM'].astype(float).sum() / frameSI2.loc[frameSI2['Dia']== dia,'THROU_USER_DEN'].astype(float).sum()
        frameSI2.loc[(frameSI2['Dia'] == dia),['4G_ACV_Muni']] = frameSI2.loc[frameSI2['Dia']== dia,'ACV_NUM'].astype(float).sum() / frameSI2.loc[frameSI2['Dia']== dia,'ACV_DEN'].astype(float).sum()
        frameSI2.loc[(frameSI2['Dia'] == dia),['4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_Muni']] = frameSI2.loc[frameSI2['Dia']== dia,'VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].astype(float).sum()
      
      frameSI2 = frameSI2.astype(str)
      lista1 = ['4G_ACD_Muni','4G_DROP_VOZ_Muni','4G_DROP_DADOS_Muni','4G_DISP_Muni','4G_THROU_USER_Muni','4G_ACV_Muni','4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_Muni']
      for i in lista1:
        frameSI2[i] = [x.replace('.', ',') for x in frameSI2[i]]
      csv_path = os.path.join(script_dir, 'export/MUNICIPIO/'+ mun +'.csv')
      frameSI2.to_csv(csv_path,index=False,header=True,sep=';')

    '''

    #frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO