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
    fields = ['Dia','Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','CELL_Fisico','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV','NOME_DO_SU']
    fields2 = ['Dia','Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','CELL_Fisico','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV','NOME_DO_SU']
    
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
    frameSI.insert(len(frameSI.columns),'Dia1',pd.to_datetime(frameSI['Dia'].astype(str), format="%d/%m/%Y"))
    frameSI = frameSI.sort_values(['Dia1'], ascending = [True])

    frameSI = frameSI.astype(str)
    lista1 = frameSI.columns
    for i in lista1:
      frameSI[i] = [x.replace(',', '.') for x in frameSI[i]]

    
    #frameSI = frameSI.loc[frameSI['Black_List'].isna()]
    frameSI['ref_key'] = frameSI['Station ID'] + frameSI['CELL_Fisico']

    ref_List = ['Municipio']
    tec_List = ['2G','3G','4G']
    col_Dia = frameSI['Dia'].unique()
    header = frameSI.columns.values.tolist()

    for ref in ref_List:
      col_ref_List = frameSI[ref].unique()
      for tec in tec_List:
        index1 = 0
        for ref_Index in col_ref_List:
          frameSI2 = frameSI.loc[(frameSI[ref] == ref_Index) & (frameSI['Tecnologia'] == tec)]
          #col_ref_List = frameSI[ref].unique()
          for dia in col_Dia:
            
            frameSI2.loc[(frameSI2['Dia'] == dia),[tec+'_Peso_ACD_'+ref]] = frameSI2.loc[(frameSI2['Dia']== dia),'Peso_ACD'].astype(float) / frameSI2.loc[frameSI2['Dia']== dia,'Peso_ACD'].astype(float).sum()
            frameSI2.loc[(frameSI2['Dia'] == dia),[tec+'_Peso_DROP_VOZ_'+ref]] = frameSI2.loc[(frameSI2['Dia']== dia),'Peso_DROP_VOZ'].astype(float) / frameSI2.loc[frameSI2['Dia']== dia,'Peso_DROP_VOZ'].astype(float).sum()
            frameSI2.loc[(frameSI2['Dia'] == dia),[tec+'_Peso_DROP_DADOS_'+ref]] = frameSI2.loc[(frameSI2['Dia']== dia),'Peso_DROP_DADOS'].astype(float) / frameSI2.loc[frameSI2['Dia']== dia,'Peso_DROP_DADOS'].astype(float).sum()
            frameSI2.loc[(frameSI2['Dia'] == dia),[tec+'_Peso_DISP_'+ref]] = frameSI2.loc[(frameSI2['Dia']== dia),'Peso_DISP'].astype(float) / frameSI2.loc[frameSI2['Dia']== dia,'Peso_DISP'].astype(float).sum()
            frameSI2.loc[(frameSI2['Dia'] == dia),[tec+'_Peso_ACV_'+ref]] = frameSI2.loc[(frameSI2['Dia']== dia),'Peso_ACV'].astype(float) / frameSI2.loc[frameSI2['Dia']== dia,'Peso_ACV'].astype(float).sum()
          frameSI2 = frameSI2.astype(str)    
          dropList = ['Dia1']
          frameSI2 = frameSI2.drop(dropList, axis=1)
          frameSI2 = frameSI2.drop_duplicates()
          frameSI2 = frameSI2.sort_values([tec+'_Peso_ACD_'+ref,tec+'_Peso_DROP_VOZ_'+ref,tec+'_Peso_DROP_DADOS_'+ref,tec+'_Peso_DISP_'+ref,tec+'_Peso_ACV_'+ref], ascending = [False,False,False,False,False])

          lista1 = frameSI2.columns
          for i in lista1:
            frameSI2[i] = [x.replace('.', ',') for x in frameSI2[i]]
          
          if index1 == 0:
            frame_ALL = frameSI2.copy()
            index1 = 1
          else:
            frame_ALL = frame_ALL.append(frameSI2,ignore_index = True)
          #print(len(frame_ALL.index))  
        #csv_path = os.path.join(script_dir, 'export/'+ ref +'/'+tec+'/'+ DateCreation + '_' + tec + '_'+ ref +'.csv')
        csv_path = os.path.join(script_dir, 'export/'+ ref +'/'+tec+'/'+ tec + '_'+ ref +'ofensores.csv')
        
        
        frame_ALL.to_csv(csv_path,index=False,header=True,sep=';')
        print(tec,'_',ref,' Saved!\n')





