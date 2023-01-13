import timeit

import MS_2G
import MS_3G
import MS_4G
import MS




print ('\nprocessing... ')
inicio = timeit.default_timer()

MS_2G.processArchive('Dia')
MS_3G.processArchive('Dia')
MS_4G.processArchive('Dia')
MS.processArchive('Dia')







fim = timeit.default_timer()
print ('duracao: %.2f' % ((fim - inicio)/60) + ' min') 
