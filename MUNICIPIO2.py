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
    fields = ['Dia','Municipio','ANF','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM']
    fields2 = ['Dia','Municipio','ANF','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM']
    
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
    lista1 = frameSI.columns
    for i in lista1:
      frameSI[i] = [x.replace(',', '.') for x in frameSI[i]]

    
    #frameSI = frameSI.loc[frameSI['Black_List'].isna()]
    frameSI = frameSI.sort_values(['Dia','Municipio','Tecnologia'], ascending = [True, True, True])

    ref_List = ['Municipio']
    tec_List = ['2G','3G','4G']
    col_Dia = frameSI['Dia'].unique()
    header = frameSI.columns.values.tolist()
    for ref in ref_List:
      col_ref_List = frameSI[ref].unique()
      for ref_Index in col_ref_List:
        for dia in col_Dia:
          for tec in tec_List:
            frameSI.loc[(frameSI['Dia'] == dia) & (frameSI[ref] == ref_Index) & (frameSI['Tecnologia'] == tec),[tec+'_ACD_'+ref]] = frameSI.loc[frameSI['Dia']== dia,'ACD_NUM'].astype(float).sum() / frameSI.loc[frameSI['Dia']== dia,'ACD_DEN'].astype(float).sum()
            frameSI.loc[(frameSI['Dia'] == dia) & (frameSI[ref] == ref_Index) & (frameSI['Tecnologia'] == tec),[tec+'_DROP_VOZ_'+ref]] = frameSI.loc[frameSI['Dia']== dia,'DROP_VOZ_NUM'].astype(float).sum() / frameSI.loc[frameSI['Dia']== dia,'DROP_VOZ_DEN'].astype(float).sum()
            frameSI.loc[(frameSI['Dia'] == dia) & (frameSI[ref] == ref_Index) & (frameSI['Tecnologia'] == tec),[tec+'_DROP_DADOS_'+ref]] = frameSI.loc[frameSI['Dia']== dia,'DROP_DADOS_NUM'].astype(float).sum() / frameSI.loc[frameSI['Dia']== dia,'DROP_DADOS_DEN'].astype(float).sum()
            frameSI.loc[(frameSI['Dia'] == dia) & (frameSI[ref] == ref_Index) & (frameSI['Tecnologia'] == tec),[tec+'_DISP_'+ref]] = frameSI.loc[frameSI['Dia']== dia,'DISP_NUM'].astype(float).sum() / frameSI.loc[frameSI['Dia']== dia,'DISP_DEN'].astype(float).sum()
            frameSI.loc[(frameSI['Dia'] == dia) & (frameSI[ref] == ref_Index) & (frameSI['Tecnologia'] == tec),[tec+'_THROU_USER_'+ref]] = frameSI.loc[frameSI['Dia']== dia,'THROU_USER_NUM'].astype(float).sum() / frameSI.loc[frameSI['Dia']== dia,'THROU_USER_DEN'].astype(float).sum()
            frameSI.loc[(frameSI['Dia'] == dia) & (frameSI[ref] == ref_Index) & (frameSI['Tecnologia'] == tec),[tec+'_ACV_'+ref]] = frameSI.loc[frameSI['Dia']== dia,'ACV_NUM'].astype(float).sum() / frameSI.loc[frameSI['Dia']== dia,'ACV_DEN'].astype(float).sum()
            frameSI.loc[(frameSI['Dia'] == dia) & (frameSI[ref] == ref_Index) & (frameSI['Tecnologia'] == tec),[tec+'_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_'+ref]] = frameSI.loc[frameSI['Dia']== dia,'VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].astype(float).sum()
          #print(tec,dia,ref_Index,ref)

 


    frameSI = frameSI.astype(str)    
    dropList = ['ANF','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','Dia1']
    frameSI = frameSI.drop(dropList, axis=1)
    frameSI = frameSI.drop_duplicates()
    
    frameSI = frameSI.astype(str)
    lista1 = frameSI.columns
    for i in lista1:
      frameSI[i] = [x.replace('.', ',') for x in frameSI[i]]
    csv_path = os.path.join(script_dir, 'export/Municipio/'+'Consolidado' +'.csv')   
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')
    print(tec,'_',ref,' Saved!\n')





