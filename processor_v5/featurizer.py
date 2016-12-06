# -*- coding: utf-8 -*-
"""
Created on Tue Nov 29 13:40:54 2016

@author: genkinjz
"""
import abc


class Featurizer(object):
    __metaclass__ = abc.ABCMeta
    
    def __init__(self):
        return

    def to_row(self, feature):
        if not self.can_featurize(feature):
            my_name = self.__class__.__name__
            print "Featurizing failed: {0} cannot featurize {1}".format(my_name,feature.name)
            return None
        return self.__do_featurize(feature)
    
    @abc.abstractmethod
    def __do_featurize(self, feature):
        return
    
    @abc.abstractmethod
    def can_featurize(self,feature):
        return
    
    @abc.abstractmethod
    def get_column_names(self):
        return
    
    #TODO: Make class comparable on Class Name