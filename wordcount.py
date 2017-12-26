#!/usr/bin/env python

import os
import sys

reload(sys)
sys.path = [os.getcwd()] + sys.path
sys.setdefaultencoding("utf-8")




class Mapper(api.Mapper):
    def map(self, context):
        fname=context.getInputSplit()
        fname1=fname.split("/")
        filename=fname1[-1]
        words = context.value.split()
        for w in words:
            context.emit(w + " @ " + filename , 1)
      
      
class Reducer(api.Reducer):
    def reduce(self, context):
      s = sum(context.values)
      context.emit(context.getInputKey(), s)

def __main__():
     pp.run_task(pp.Factory(Mapper, Reducer))

