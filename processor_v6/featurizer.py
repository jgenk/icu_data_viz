# -*- coding: utf-8 -*-
"""
Created on Thu Dec 01 16:28:31 2016

@author: genkinjz
"""
import abc
from functools import total_ordering

@total_ordering
class Featurizer(object):
    __metaclass__ = abc.ABCMeta
    
    def __init__(self):
        return

        
    def featurize(self, data,**kwargs):
        can_featurize,error = self.can_featurize(data, **kwargs)
        if not can_featurize:
            print "Featurizing failed ({0}): {1}".format(self.__str__, error)
            return None
        return self.__do_featurize(data,**kwargs)
    
    def __eq__(self, other):
        return self.__str__ == other.__str__

    def __lt__(self, other):
        return self.__str__ < other.__str__

    def __str__(self):
        return self.__class__.__name__
        
    @abc.abstractmethod
    def can_featurize(self, data,**kwargs):
        return
        
    @abc.abstractmethod
    def __do_featurize(self, data,**kwargs):
        return

