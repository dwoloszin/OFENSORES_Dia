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
    fields = [Agregacao,periodo,'Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Banda','Cell','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Gbyte)','TRAFEGO_VOZ_TIM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','INTER1','INTER2']
    fields2 = [Agregacao,periodo,'Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Banda','Cell','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Gbyte)','TRAFEGO_VOZ_TIM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','INTER1','INTER2']
    
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
    frameSI = frameSI.loc[~frameSI['Cluster'].isna()]

    frameSI = frameSI.astype(str)
    lista1 = frameSI.columns
    for i in lista1:
      frameSI[i] = [x.replace(',', '.') for x in frameSI[i]]

    numberColumn = ['Peso_DISP','Peso_ACV','Peso_ACD','Peso_DROP_DADOS','Peso_DROP_VOZ','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','INTER1','INTER2','VOLUME_DADOS_DLUL_ALLOP(Gbyte)','TRAFEGO_VOZ_TIM']
    for i in numberColumn:
      frameSI[i] = frameSI[i].astype(float)
    #df.groupby(['att1', 'att2']).agg({'att1': "count", 'att3': "sum",'att4': 'mean'})
  
    frameSI_ = frameSI.groupby([Agregacao,periodo,'Tecnologia']).agg({'Peso_DISP':'sum','Peso_ACV':'sum','Peso_ACD':'sum','Peso_DROP_DADOS':'sum','Peso_DROP_VOZ':'sum','ACD_DEN':'sum','ACD_NUM':'sum','DROP_VOZ_DEN':'sum','DROP_VOZ_NUM':'sum','DROP_DADOS_DEN':'sum','DROP_DADOS_NUM':'sum','DISP_DEN':'sum','DISP_NUM':'sum','THROU_USER_DEN':'sum','THROU_USER_NUM':'sum','ACV_DEN':'sum','ACV_NUM':'sum','INTER1':'max','INTER2':'max','VOLUME_DADOS_DLUL_ALLOP(Gbyte)':'sum','TRAFEGO_VOZ_TIM':'sum'}).reset_index()
    frameSI_['ref1'] = frameSI_[Agregacao] + frameSI_[periodo] + frameSI_['Tecnologia']
    '''
    dropList2 = ['ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM']
    frameSI = frameSI.drop(dropList2, axis=1)
    '''
    frameSI = frameSI.groupby([Agregacao,periodo,'BTS/NodeB/ENodeB','Tecnologia']).agg({'Peso_DISP':'sum','Peso_ACV':'sum','Peso_ACD':'sum','Peso_DROP_DADOS':'sum','Peso_DROP_VOZ':'sum','ACD_DEN':'sum','ACD_NUM':'sum','DROP_VOZ_DEN':'sum','DROP_VOZ_NUM':'sum','DROP_DADOS_DEN':'sum','DROP_DADOS_NUM':'sum','DISP_DEN':'sum','DISP_NUM':'sum','THROU_USER_DEN':'sum','THROU_USER_NUM':'sum','ACV_DEN':'sum','ACV_NUM':'sum','INTER1':'max','INTER2':'max','VOLUME_DADOS_DLUL_ALLOP(Gbyte)':'sum','TRAFEGO_VOZ_TIM':'sum'}).reset_index()
    frameSI['ref2'] = frameSI[Agregacao] + frameSI[periodo] + frameSI['Tecnologia']

    frameSI['ACD'] = frameSI['ACD_NUM'] / frameSI['ACD_DEN']
    frameSI['ACV'] = frameSI['ACV_NUM'] / frameSI['ACV_DEN']
    frameSI['DISP'] = frameSI['DISP_NUM'] / frameSI['DISP_DEN']
    frameSI['DROP_VOZ'] = frameSI['DROP_VOZ_NUM'] / frameSI['DROP_VOZ_DEN']
    frameSI['DROP_DADOS'] = frameSI['DROP_DADOS_NUM'] / frameSI['DROP_DADOS_DEN']
    #frameSI['THROU_USER'] = frameSI['THROU_USER_NUM'] / frameSI['THROU_USER_DEN']
    frameSI['THROU_USER'] = frameSI['THROU_USER_NUM'].div(frameSI['THROU_USER_DEN']).round(0)
    frameSI['THROU_USER']  = frameSI['THROU_USER'].astype('int', errors='ignore')
    dropList2 = ['ACD_NUM','ACD_DEN','ACV_NUM','ACV_DEN','DISP_NUM','DISP_DEN','DROP_VOZ_NUM','DROP_VOZ_DEN','DROP_DADOS_NUM','DROP_DADOS_DEN','THROU_USER_NUM','THROU_USER_DEN']
    frameSI = frameSI.drop(dropList2, axis=1)
    dropList2.append(Agregacao)
    dropList2.append(periodo)
    dropList2.append('Tecnologia')
    frameSI_ = frameSI_.drop(dropList2, axis=1)
    frameSI_.rename(columns={'Peso_DISP':'Peso_DISP_','Peso_ACV':'Peso_ACV_','Peso_ACD':'Peso_ACD_','Peso_DROP_DADOS':'Peso_DROP_DADOS_','Peso_DROP_VOZ':'Peso_DROP_VOZ_','INTER1':'INTER1_','INTER2':'INTER2_','VOLUME_DADOS_DLUL_ALLOP(Gbyte)':'VOLUME_DADOS_DLUL_ALLOP(Gbyte)_','TRAFEGO_VOZ_TIM':'TRAFEGO_VOZ_TIM_'},inplace=True)

    frameSI = pd.merge(frameSI,frameSI_, how='left',left_on=['ref2'],right_on=['ref1'])
    #frameSI_ = frameSI_.drop(['ref1'], axis=1)

    frameSI['Peso_DISP'] = frameSI['Peso_DISP'].div(frameSI['Peso_DISP_']).round(2)
    frameSI['Peso_ACV'] = frameSI['Peso_ACV'].div(frameSI['Peso_ACV_']).round(2)
    frameSI['Peso_ACD'] = frameSI['Peso_ACD'].div(frameSI['Peso_ACD_']).round(2)
    frameSI['Peso_DROP_DADOS'] = frameSI['Peso_DROP_DADOS'].div(frameSI['Peso_DROP_DADOS_']).round(2)
    frameSI['Peso_DROP_VOZ'] = frameSI['Peso_DROP_VOZ'].div(frameSI['Peso_DROP_VOZ_']).round(2)

    #'VOLUME_DADOS_DLUL_ALLOP(Gbyte)':'VOLUME_DADOS_DLUL_ALLOP(Gbyte)_','TRAFEGO_VOZ_TIM'
    frameSI['Peso_Volume'] = frameSI['VOLUME_DADOS_DLUL_ALLOP(Gbyte)'].div(frameSI['VOLUME_DADOS_DLUL_ALLOP(Gbyte)_']).round(2)
    frameSI['Peso_trafegoVoz'] = frameSI['TRAFEGO_VOZ_TIM'].div(frameSI['TRAFEGO_VOZ_TIM_']).round(2)
    #frameSI['Peso_INTER1'] = frameSI['INTER1'].sub(frameSI['INTER1_']).round(0) * -1
    #frameSI['Peso_INTER2'] = frameSI['INTER2'].sub(frameSI['INTER2_']).round(0) *-1

    #normalize volume
    frameSI['VOLUME_DADOS_DLUL_ALLOP(Gbyte)'] = frameSI['VOLUME_DADOS_DLUL_ALLOP(Gbyte)'].round(2)
    frameSI['TRAFEGO_VOZ_TIM'] = frameSI['TRAFEGO_VOZ_TIM'].round(2)

    fropList = ['Peso_DISP_','Peso_ACV_','Peso_ACD_','Peso_DROP_DADOS_','Peso_DROP_VOZ_','ref1','ref2','INTER1_','INTER2_','TRAFEGO_VOZ_TIM_','VOLUME_DADOS_DLUL_ALLOP(Gbyte)_']
    frameSI = frameSI.drop(fropList, axis=1)
    

    frameSILastSeen = frameSI.copy()
    fropList1 = ['Cluster','Tecnologia','Peso_DISP','Peso_ACV','Peso_ACD','Peso_DROP_DADOS','Peso_DROP_VOZ','ACD','ACV','DISP','DROP_VOZ','DROP_DADOS','THROU_USER','INTER1','INTER2','VOLUME_DADOS_DLUL_ALLOP(Gbyte)','TRAFEGO_VOZ_TIM','Peso_Volume','Peso_trafegoVoz']
    frameSILastSeen = frameSILastSeen.drop(fropList1, axis=1)
    frameSILastSeen = frameSILastSeen.drop_duplicates(subset=['BTS/NodeB/ENodeB'], keep = 'last')
    lastdata =  frameSILastSeen['Data'].iloc[0]

 

    frameSILastSeen = frameSILastSeen.loc[(frameSILastSeen['Data'] != lastdata)]
    frameSILastSeen = frameSILastSeen.reset_index()


    

    frameSI.loc[frameSI['INTER1'] == 0,['INTER1']] = ''
    frameSI.loc[frameSI['INTER2'] == 0,['INTER2']] = ''
    print(frameSILastSeen)
    frameSI = frameSI.sort_values(['Data'], ascending = [True])
    csv_path = os.path.join(script_dir, 'export/PBI/'+'OFENSOR'+'.csv')
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

    frameSILastSeen = frameSILastSeen.sort_values(['Data'], ascending = [True])
    csv_path2 = os.path.join(script_dir, 'export/PBI/'+'HoudiniSite'+'.csv')
    frameSILastSeen.to_csv(csv_path2,index=False,header=True,sep=';')
    
    
    '''
    ref_List = [Agregacao]
    tec_List = ['2G','3G','4G']
    col_Dia = frameSI[periodo].unique()
    col_Site = frameSI['BTS/NodeB/ENodeB'].unique()
    header = frameSI.columns.values.tolist()
    for ref in ref_List:
      col_ref_List = frameSI[ref].unique()
      for tec in tec_List:
        index1 = 0
        for ref_Index in col_ref_List:
          frameSI2 = frameSI.loc[(frameSI[ref] == ref_Index) & (frameSI['Tecnologia'] == tec)]
          for dia in col_Dia:
            for site in col_Site:
              frameSI2.loc[(frameSI2[periodo] == dia) & (frameSI2['BTS/NodeB/ENodeB'] == site),[tec+'_Peso_ACD_'+ref]] = frameSI2.loc[frameSI2[periodo]== dia,'Peso_ACD'].astype(float).sum()
  
          frameSI2 = frameSI2.astype(str)    
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
          # 
          #  
        #csv_path = os.path.join(script_dir, 'export/'+ ref +'/'+tec+'/'+ tec + '_'+ ref +'.csv')
        #csv_path = os.path.join(script_dir, 'export/'+ ref +'/'+tec+'/'+ DateCreation + '_' + tec + '_'+ ref +'.csv')
        csv_path = os.path.join(script_dir, 'export/'+ ref +'_W_Ofensor'+'/'+tec+'/'+ tec + '_'+ ref +'.csv')
        frame_ALL = frame_ALL.loc[frame_ALL['Cluster'] != 'nan']
        frame_ALL.to_csv(csv_path,index=False,header=True,sep=';')
        print(tec,'_',ref,' Saved!\n')
            
    '''      



