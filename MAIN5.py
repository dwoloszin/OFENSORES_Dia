import timeit
import os
import sys
import pandas as pd
import datetime
import ImportDF
import numpy as np


print ('\nprocessing... ')
inicio = timeit.default_timer()
script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')


ref1 = ['ANF']
ref2 = ['2G','3G','4G']
ref3 = ['_ACD_','_DROP_VOZ_','_DROP_DADOS_','_DISP_','_THROU_USER_','_ACV_','_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_','_ACD_(m)','_DROP_VOZ_(m)','_DROP_DADOS_(m)','_DISP_(m)','_THROU_USER_(m)','_ACV_(m)','_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_(m)']

index1 = 0
for i in ref1:
  ght = 'Dia'+ i
  for j in ref2:
    if index1 == 0:
      header = ['Dia',ref1[0],'Tecnologia']
      for h in ref3:
        header.append(j+h+i)
      frameSI = ImportDF.ImportDF(header,'/export/'+i+'/'+j)
      frameSI[ght] = frameSI['Dia'] + frameSI[i]
      index1 = 1
    else:
      header = ['Dia',ref1[0]]
      for k in ref3:
        header.append(j+k+i)
      frameSI2 = ImportDF.ImportDF(header,'/export/'+i+'/'+j)
      frameSI2['Dia'+ref1[0]] = frameSI2['Dia'] + frameSI2[ref1[0]]
      frameSI2 = frameSI2.drop(['Dia',ref1[0]], axis=1)


      frameSI = pd.merge(frameSI,frameSI2, how='left',left_on=['Dia'+ref1[0]],right_on=['Dia'+ref1[0]])

      #frameSI['OffLoad4G'] = frameSI['4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0]] / (frameSI['4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0]] +frameSI['2G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0]]+frameSI['3G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0]])
      #4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_ANF
      #4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_ANF
  # OFFLoad4G
  frameSI = frameSI.fillna(0)
  #frameSI = frameSI.replace('nan', 0)

  frameSI = frameSI.astype(str)
  lista1 = ['2G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0],'3G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0],'4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0]]
  for i in lista1:
    frameSI[i] = [x.replace(',', '.') for x in frameSI[i]]
  frameSI['OffLoad4G'] = frameSI['4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0]].astype(float) / (frameSI['4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0]].astype(float) +frameSI['2G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0]].astype(float) + frameSI['3G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0]].astype(float))
  col_ref_List = frameSI[ref1[0]].unique()
  for ref_Index in col_ref_List:
    frameSI.loc[frameSI[ref1[0]] == ref_Index,['OffLoad4G_(m)']] = frameSI.loc[frameSI[ref1[0]] == ref_Index,'OffLoad4G'].median()

  frameSI = frameSI.astype(str)
  lista1 = ['2G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0],'3G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0],'4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_' + ref1[0],'OffLoad4G','OffLoad4G_(m)']
  for i in lista1:
    frameSI[i] = [x.replace('.', ',') for x in frameSI[i]]


  Data_Inicio = frameSI.iloc[0]['Dia'].split('/')
  new_list = list(reversed(Data_Inicio))
  Data_Inicio = ''.join(str(e) for e in new_list)


  Data_FIM = frameSI.iloc[-1]['Dia'].split('/')
  new_list = list(reversed(Data_FIM))
  Data_FIM = ''.join(str(e) for e in new_list)
  print (Data_FIM)

  
  csv_path = os.path.join(script_dir, 'export/'+'Process_All/' + Data_Inicio + '-' + Data_FIM + '_' + ref1[0] +'_Process_All' +'.csv')
          
  frameSI.to_csv(csv_path,index=False,header=True,sep=';')


fim = timeit.default_timer()
print ('duracao: %.2f' % ((fim - inicio)/60) + ' min') 
