#!/usr/bin/env python

import os
import sys

reload(sys)
sys.path = [os.getcwd()] + sys.path
sys.setdefaultencoding("utf-8")


import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp

class Mapper(api.Mapper):
       def map(self, context):
           for key  in context.value.split(' '):
               if '@' in key:
                   keyWord=key.split('@')[0]
                   context.emit(keyWord, 1)
       

class Reducer(api.Reducer):
      def reduce(self, context):
          s = sum(context.values)
          context.emit(context.key, s)

def __main__():
     pp.run_task(pp.Factory(Mapper, Reducer))

