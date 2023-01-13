import timeit
import os
import sys
import pandas as pd
import datetime
import ImportDF



def processArchive(frameSI):
  kpiList = ['_ACD_','_DROP_VOZ_','_DROP_DADOS_','_THROU_USER_','_ACV_','_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_','_TRAFEGO_VOZ_TIM_','_USERS_','_INTER1_','_INTER2_','_DISP_']
  tecList = ['2G','3G','4G']
  for i in tecList:
    for j in kpiList:
      frameSI.loc[(frameSI[i+'_DISP_'+'Cluster'] == '0'),[i+j+'Cluster']] = ''

  #tratar inter zero
  frameSI = interf(frameSI)

  
  return frameSI



def interf(frameSI):
  kpiList = ['_INTER1_','_INTER2_']
  tecList = ['2G','3G','4G']
  for i in tecList:
    for j in kpiList:
      frameSI.loc[(frameSI[i+j+'Cluster'] == '0') | (frameSI[i+j+'Cluster'] == 0),[i+j+'Cluster']] = ''
  
  return frameSI