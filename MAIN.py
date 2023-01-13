import First
import Second
import Ofensor
import timeit

import MS_2G
import MS_3G
import MS_4G
import MS
import MobileSite

import MS_2G_ALTAIA
import MS_3G_ALTAIA
import MS_4G_ALTAIA



print ('\nprocessing... ')
inicio = timeit.default_timer()

#Ofensor.processArchive('Cluster','Data')



MobileSite.processArchive()


#Deletar arqiovos do MS da pasta MS

agrr = ['Data']
#cluster = ['Cluster','ANF','Municipio']
#cluster = ['Municipio']
#cluster = ['ANF']
cluster = ['Cluster']
for i in agrr:
  
  MS_2G_ALTAIA.processArchive(i) #Semana do Ano
  MS_3G_ALTAIA.processArchive(i) 
  MS_4G_ALTAIA.processArchive(i)
  MS.processArchive(i)
  
  for j in cluster:
    First.processArchive(j,i)
    Second.processArchive(j,i)


Ofensor.processArchive('Cluster','Data')



fim = timeit.default_timer()
print ('duracao: %.2f' % ((fim - inicio)/60) + ' min') 

