# -*- coding: utf-8 -*-
"""
Created on Thu Dec 01 16:28:31 2016

@author: genkinjz
"""
import abc
from functools import total_ordering

@total_ordering
class Featurizer(object):
    """
    Abstract class, facilitates transforming raw data into features. Enforces
    checking if the featurizer can featurize the data prior to trying to featurize
    it. Also implements total_ordering decorator so that featurizers can be
    comparable in a set
    """
    __metaclass__ = abc.ABCMeta
    
    def __init__(self):
        return

        
    def featurize(self, data,**kwargs):
        """
        Transform some data into a single pd.Series with column names corresponding
        to the different features produced from this data. This method checks if you
        can_featurize this data prior to executing the featurize.
        
        **kwargs is defined in subclasses
        
        Return: pd.Series with index values corresponding to feature names
        """
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
        """
        Checks if this featurizer can featurize the data.
        
        Return True if it can.
        """
        return
        
    @abc.abstractmethod
    def __do_featurize(self, data,**kwargs):
        """
        Actually performs the featurizing process.
        
        Return: pd.Series with index values corresponding to feature names
        
        """
        return

