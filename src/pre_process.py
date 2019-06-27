# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 14:55:56 2019

@author: kumar.shivam
"""
import sys
import pandas as pd
import pyodbc
import json
import sql_queries

class pre_process:
    """
    Preprocess includes connection to the SQL server, reading all required data from the AIR CEDE table.
    These are queries which joins data from multiple table to reflect into one dataframe.
    It also includes reading column names from pre-defined csv and creating black OED table.
    """
    def read_sql_data(self,logger): 
        """
        Reading SQL data from table tLocTerm, tExposureSet, tLocation, tLocFeature, tLayerConditionLocationXref,
        tLocation and tContract.
        These data are joined as per rquirement shared in the documentation.
        """
        try:
            with open(r"..\augmentations\connection_string1.json") as json_file:  
                self.dbconn = json.load(json_file)            
            self.connection_string = r'Driver='+self.dbconn['Driver']+';Server='+self.dbconn['Server']+';Database='+self.dbconn['Database']+';Trusted_Connection='+self.dbconn['TrustedConnection']+';UID='+self.dbconn['ID']+';PWD='+self.dbconn['PWD']+';'
            self.sql_conn_AIR = pyodbc.connect(self.connection_string)
            logger.info('Successfully Connected to CEDE AIR Database')
        except Exception as e:
            logger.info('Issue in Database Connection')
            logger.error(e)
            print("Error Check Log file")
            sys.exit(0)
        
        try:
            self.query_tLocTm_tLoc_tExSet_tLocFeat =  sql_queries.query_tLocTm_tLoc_tExSet_tLocFeat                        
            self.AIR_location_file = pd.read_sql(self.query_tLocTm_tLoc_tExSet_tLocFeat, self.sql_conn_AIR) 
            logger.info('Successfully read data from AIR DB for Tlocterm, Tloc, Texpset, tlocFeat')
        except Exception as e:
            logger.info('Issue in reading data from AIR DB for Tlocterm, Tloc, Texpset, tlocFeat')
            logger.error(e)
            print("Error Check Log file")
            sys.exit(0)
          
        try:    
            self.query_tLoc_tContr = sql_queries.query_tLoc_tContr       
            self.ContractID  = pd.read_sql(self.query_tLoc_tContr, self.sql_conn_AIR)
            self.AIR_location_file = self.AIR_location_file.join(self.ContractID)  
            logger.info('Successfully read data from AIR DB for ContractID from tloc, tcontract')                  
        except Exception as e:   
            logger.info('Issue in reading data from AIR DB for ContractID from tloc, tcontract')   
            logger.error(e)   
            print("Error Check Log file") 
            sys.exit(0)              
                       
        try:
            self.query_tlclx_tlc = sql_queries.query_tlclx_tlc
            self.LayerConditionSID  = pd.read_sql(self.query_tlclx_tlc, self.sql_conn_AIR)
            self.AIR_location_file = self.AIR_location_file.join(self.LayerConditionSID) 
            self.sql_conn_AIR.close()
            return self.AIR_location_file
            logger.info('Successfully read data from AIR DB for LayerconditionSID. CondNumber from tlocCondXref, tLayerCondition')                  
        except Exception as e:   
            logger.info('Issue in reading data from AIR DB for LayerconditionSID. CondNumber from tlocCondXref, tLayerCondition')                  
            logger.error(e) 
            print("Error Check Log file")
            sys.exit(0)

    
    def OED_file_preprocess(self,logger):
        """
        Reading the column names required for the OED files from location column names csv and creating
        the empty dataframe with the same column names,
        """
        try:
            self.OED_location_column_names = pd.read_csv(r"..\augmentations\location_column_names.csv",header = 0, index_col=0)
            self.OED_location_column_names_list = self.OED_location_column_names['ColumnNames'].tolist()            
            self.OED_location_file = pd.DataFrame(columns = self.OED_location_column_names_list)
            return self.OED_location_file  
            logger.info('Successfully created blank OED file')                  
        except Exception as e:
            logger.info('Issue in creating blank OED file') 
            logger.error(e)  
            print("Error Check Log file") 
            sys.exit(0) 
                       