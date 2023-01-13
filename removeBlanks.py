import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import warnings
warnings.simplefilter("ignore")




def remove(data):
  x = ''
  list1 = ['0','1','2','3','4','5','6','7','8','9']
  for i in str(data):
    if i in list1:
      x += i
  return x    
