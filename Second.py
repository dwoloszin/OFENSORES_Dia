import timeit
import os
import sys
import pandas as pd
import datetime
import ImportDF
import WEEK_PBIConfig
import TratarZeros


print ('\nprocessing... ')
inicio = timeit.default_timer()
script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')

def processArchive(Agregacao,periodo):
  ref1 = [Agregacao]
  ref2 = ['2G','3G','4G']
  ref3 = ['_ACD_','_DROP_VOZ_','_DROP_DADOS_','_DISP_','_THROU_USER_','_ACV_','_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_','_TRAFEGO_VOZ_TIM_','_USERS_','_INTER1_','_INTER2_']#,'_CRITICO_','_ALERTA_','_BOM_','_OCUPACAO_'

  '''
  index1 = 0
  for i in ref1:
    ght = periodo+ i
    for j in ref2:
      if index1 == 0:
        header = [periodo,ref1[0]]
        for h in ref3:
          header.append(j+h+i)
        frameSI = ImportDF.ImportDF(header,'/export/'+i+'_W/'+j)
        
        frameSI[ght] = frameSI[periodo] + frameSI[i]
        index1 = 1
      else:
        header = [periodo,ref1[0]]
        for k in ref3:
          header.append(j+k+i)
        frameSI2 = ImportDF.ImportDF(header,'/export/'+i+'_W/'+j)
        frameSI2[periodo+ref1[0]] = frameSI2[periodo] + frameSI2[ref1[0]]
        frameSI2 = frameSI2.drop([periodo,ref1[0]], axis=1)

        frameSI = pd.merge(frameSI,frameSI2, how='left',left_on=[periodo+ref1[0]],right_on=[periodo+ref1[0]])
  '''
  index1 = 0
  for i in ref1:
    ght = periodo+ i
    for j in ref2:
      if index1 == 0:
        header = [periodo,ref1[0]]
        frameSI = ImportDF.ImportDF(header,'/export/'+i+'_W/'+'2G')
        frameSI3G = ImportDF.ImportDF(header,'/export/'+i+'_W/'+'3G')
        frameSI4G = ImportDF.ImportDF(header,'/export/'+i+'_W/'+'4G')
        frameSI = frameSI.append(frameSI3G, ignore_index=True)
        frameSI = frameSI.append(frameSI4G, ignore_index=True)
        frameSI = frameSI.drop_duplicates()
        frameSI[ght] = frameSI[periodo] + frameSI[i]
        index1 = 1
      
      header = [periodo,ref1[0]]
      for k in ref3:
        header.append(j+k+i)
      frameSI2 = ImportDF.ImportDF(header,'/export/'+i+'_W/'+j)
      frameSI2[periodo+ref1[0]] = frameSI2[periodo] + frameSI2[ref1[0]]
      frameSI2 = frameSI2.drop([periodo,ref1[0]], axis=1)

      frameSI = pd.merge(frameSI,frameSI2, how='left',left_on=[periodo+ref1[0]],right_on=[periodo+ref1[0]])








    frameSI = frameSI.fillna(0)
    # OFFLoad4G
    frameSI = frameSI.astype(str)
    lista1 = ['2G_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_' + ref1[0],'3G_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_' + ref1[0],'4G_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_' + ref1[0]]
    for i in lista1:
      frameSI[i] = [x.replace(',', '.') for x in frameSI[i]]
    frameSI['OffLoad4G'] = frameSI['4G_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_' + ref1[0]].astype(float) / (frameSI['4G_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_' + ref1[0]].astype(float) +frameSI['2G_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_' + ref1[0]].astype(float) + frameSI['3G_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_' + ref1[0]].astype(float))

    
    
    frameSI = frameSI.astype(str)
    lista1 = ['2G_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_' + ref1[0],'3G_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_' + ref1[0],'4G_VOLUME_DADOS_DLUL_ALLOP(Gbyte)_' + ref1[0]]#,'OffLoad4G','OffLoad4G_(m)'
    for i in lista1:
      frameSI[i] = frameSI[i].astype(float).round(2).astype(str)
      #frameSI[i] = frameSI[i].astype(str)
      frameSI[i] = [x.replace('.', ',') for x in frameSI[i]]

    #frameSI['TRAFEGO_VOZ_TIM'] = frameSI['TRAFEGO_VOZ_TIM'].round(2)

    # OFFLoad4G-VOZ
    frameSI = frameSI.astype(str)
    lista1 = ['2G_TRAFEGO_VOZ_TIM_' + ref1[0],'3G_TRAFEGO_VOZ_TIM_' + ref1[0],'4G_TRAFEGO_VOZ_TIM_' + ref1[0]]
    for i in lista1:
      frameSI[i] = [x.replace(',', '.') for x in frameSI[i]]
    frameSI['OffLoad4G_Voz'] = frameSI['4G_TRAFEGO_VOZ_TIM_' + ref1[0]].astype(float) / (frameSI['4G_TRAFEGO_VOZ_TIM_' + ref1[0]].astype(float) +frameSI['2G_TRAFEGO_VOZ_TIM_' + ref1[0]].astype(float) + frameSI['3G_TRAFEGO_VOZ_TIM_' + ref1[0]].astype(float))
    
    frameSI = frameSI.astype(str)
    lista1 = ['2G_TRAFEGO_VOZ_TIM_' + ref1[0],'3G_TRAFEGO_VOZ_TIM_' + ref1[0],'4G_TRAFEGO_VOZ_TIM_' + ref1[0],'OffLoad4G_Voz','OffLoad4G']#,'OffLoad4G_Voz','OffLoad4G_Voz(m)'
    
    
    for i in lista1:
      frameSI[i] = frameSI[i].astype(float).round(2).astype(str)
      frameSI[i] = [x.replace('.', ',') for x in frameSI[i]]
    

    Data_Inicio = frameSI.iloc[0][periodo].split('/')
    new_list = list(reversed(Data_Inicio))
    Data_Inicio = ''.join(str(e) for e in new_list)
    Data_Inicio = Data_Inicio.replace(':','-')


    Data_FIM = frameSI.iloc[-1][periodo].split('/')
    new_list = list(reversed(Data_FIM))
    Data_FIM = ''.join(str(e) for e in new_list)
    Data_FIM = Data_FIM.replace(':','-')
    print (Data_Inicio,Data_FIM)

     



    #dropList = ['2G_CRITICO_NOME_DO_SU','2G_ALERTA_NOME_DO_SU','2G_BOM_NOME_DO_SU','2G_OCUPACAO_NOME_DO_SU','SemanaNOME_DO_SU','3G_CRITICO_NOME_DO_SU','3G_ALERTA_NOME_DO_SU','3G_BOM_NOME_DO_SU','3G_OCUPACAO_NOME_DO_SU']
    #frameSI = frameSI.drop(dropList, axis=1)

    

    #Tecnologia

    #frameSI.rename({'Data': 'Dia'}, axis=1, inplace=True)
    #print(frameSI.columns)
    frameSI = frameSI.drop([periodo+Agregacao], axis=1)
    print(Agregacao,periodo)
    frameSI = frameSI.sort_values([periodo,Agregacao], ascending = [True,True])
    
    csv_path = os.path.join(script_dir, 'export/'+'Process_All_W/' +Agregacao+'/'+ Data_Inicio +'_'+ Data_FIM + ref1[0] +'_Process_All_W' +'.csv')
    frameSI = frameSI.drop_duplicates()
    #tratar zeros
    frameSI = TratarZeros.processArchive(frameSI)
    frameSI = WEEK_PBIConfig.processArchive(frameSI)        
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')


fim = timeit.default_timer()
print ('duracao: %.2f' % ((fim - inicio)/60) + ' min') 
