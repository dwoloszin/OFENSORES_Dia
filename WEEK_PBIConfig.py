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



def processArchive(frameSI2):

    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/PBI/'+'Consolidado'+'.csv')

    frameSI = frameSI2.copy()

    #frameSI[["a", "b"]] = frameSI[["a", "b"]].apply(pd.to_numeric)
    teclist = ['2G','3G','4G']
    for i in teclist:
      cleanINT = ['_THROU_USER_','_USERS_','_INTER1_','_INTER2_']
      for j in cleanINT:
        frameSI[i+j+'Cluster'] = frameSI[i+j+'Cluster'].str.split(',').str[0]


    lista1 = frameSI.columns
    for j in lista1:
      frameSI[j] = [x.replace(',', '.') for x in frameSI[j]]


    #numberlist
    KeepListCompared = ['Data','Cluster']
    locationBase_comparePMO = list(frameSI.columns)
    NumberList = list(set(locationBase_comparePMO)^set(KeepListCompared))


    # numbers
    #Numbers = ['2G_ACD_Cluster','2G_DROP_VOZ_Cluster','2G_DROP_DADOS_Cluster','2G_DISP_Cluster','2G_ACV_Cluster','2G_INTER1_Cluster','2G_INTER2_Cluster','3G_ACD_Cluster','3G_DROP_VOZ_Cluster','3G_DROP_DADOS_Cluster','3G_DISP_Cluster','3G_ACV_Cluster','3G_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_Cluster','3G_TRAFEGO_VOZ_TIM_Cluster','3G_USERS_Cluster','3G_INTER1_Cluster','3G_INTER2_Cluster','4G_ACD_Cluster','4G_DROP_VOZ_Cluster','4G_DROP_DADOS_Cluster','4G_DISP_Cluster','4G_THROU_USER_Cluster','4G_ACV_Cluster','4G_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_Cluster','4G_TRAFEGO_VOZ_TIM_Cluster','4G_USERS_Cluster','4G_INTER1_Cluster','4G_INTER2_Cluster','OffLoad4G','OffLoad4G_Voz']
    frameSI[NumberList] = frameSI[NumberList].apply(pd.to_numeric, errors='coerce')

    #frameSI.to_excel(csv_path,engine='xlsxwriter',index=False)
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')
    return frameSI2
#Baixar arquivo como csv, alterAR CODIGO