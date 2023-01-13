import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import RemoveDuplcates
import warnings
import ImportDF
warnings.simplefilter("ignore")

def processArchive(periodo):
    fields = [periodo,'Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Cell','Banda','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','TRAFEGO_VOZ_TIM','USERS','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV','INTER1','INTER2']
    fields2 = [periodo,'Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Cell','Banda','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Gbyte)','TRAFEGO_VOZ_TIM','USERS','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV','INTER1','INTER2']
    
    pathImport = '/export/MS'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/MS_ALL/' + archiveName+'.csv')
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
    frameSI = frameSI.astype(str)

    MobileSite = ImportDF.ImportDF(['LOCATION','NAME','CIDADE'],'/export/MOBILESITE')
    frameSI = pd.merge(frameSI,MobileSite, how='left',left_on=['BTS/NodeB/ENodeB'],right_on=['NAME'])
    frameSI.loc[frameSI['Station ID'] == '0',['Station ID']] = frameSI['LOCATION']
    frameSI.loc[frameSI['Municipio'] == '0',['Municipio']] = frameSI['CIDADE']

    frameSI = frameSI.drop(['LOCATION','CIDADE'], axis=1)

 

    Cluster = ImportDF.ImportDF(['END ID','Cluster','Agr'],'/import/Cluster')
    print(Cluster)
    frameSI = pd.merge(frameSI,Cluster, how='left',left_on=['Station ID'],right_on=['END ID'])
    #frameSI.loc[((frameSI['NAME'].str.contains('Y')) | (frameSI['NAME'].str.contains('X'))),['Cluster']] = 'Loja-' + frameSI['Cluster']

    # inclusão de LOJA para smallcell em shoppings
    targets = ['SPX','SPY']
    frameSI.loc[(frameSI['NAME'].astype(str).apply(lambda sentence: any(word in sentence for word in targets))) & (~frameSI['Cluster'].isna()) &(frameSI['Agr'].isin(['SHOPPING[Agr]','LOJA[Agr]'])),['Cluster']] = 'Loja-' + frameSI['Cluster']
    frameSI.loc[(frameSI['NAME'].astype(str).apply(lambda sentence: any(word in sentence for word in targets))) & (~frameSI['Cluster'].isna()) &(frameSI['Agr'].isin(['SHOPPING[Agr]','LOJA[Agr]'])),['Agr']] = 'LOJA[Agr]'

   



    frameSI['Ref1'] = frameSI[periodo] + frameSI['BTS/NodeB/ENodeB']







    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO