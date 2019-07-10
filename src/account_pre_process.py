# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 11:36:26 2019

@author: kumar.shivam
"""
import ConfigParser
from constants import constants
from db_helper import dbhelper
import sys

class AIR_base_file:    
    def AIR_account_read(self,logger):   
        """
        This function is to create AIR account consolidated data.
        This data is used as base dataframe and mapped through various mappings in subsequent steps.
        """
        try:
            config = ConfigParser.ConfigParser()
            config.read(constants.CONFIG_FILE_PATH)
            self.connection_string = r'Driver={SQL Server};Server='+config.get('dbconnection', 'Server')+';Database='+config.get('dbconnection', 'Database')+';Trusted_Connection='+config.get('dbconnection', 'TrustedConnection')+';UID='+config.get('dbconnection', 'ID')+';PWD='+config.get('dbconnection', 'PWD')+';'
            
            self.query_TLC_TLON = config.get(constants.ACCOUNT_QUERY,constants.TLC_TLON_TEXPSET) 
            self.ExposureSetName  = dbhelper().data_reader(self.query_TLC_TLON,self.connection_string,None,logger) 
            
            self.query_TLC_TC = config.get(constants.ACCOUNT_QUERY,constants.TLC_TC)
            self.ContractID = dbhelper().data_reader(self.query_TLC_TC,self.connection_string,None,logger) 
            
            self.query_TLC_TL = config.get(constants.ACCOUNT_QUERY,constants.TLC_TL)
            self.LayerID = dbhelper().data_reader(self.query_TLC_TL, self.connection_string,None,logger)         
            
            self.query_TLC_DISTINCT = config.get(constants.ACCOUNT_QUERY,constants.TLC_DISTINCT)
            self.Condname_num = dbhelper().data_reader(self.query_TLC_DISTINCT, self.connection_string,None,logger) 
           
            self.AIR_account_filetmp = self.ExposureSetName.join(self.ContractID)
            self.AIR_account_filetmp = self.AIR_account_filetmp.join(self.Condname_num)
            self.AIR_account_file = self.AIR_account_filetmp.join(self.LayerID)
            return self.AIR_account_file 
            logger.info('Successfully read AIR DB and created account file')                 
        except Exception as e:
            logger.info('Issue in reading AIR DB and creating account file')
            logger.error(e)
            print("Error Check Log file")
            sys.exit(0)
         