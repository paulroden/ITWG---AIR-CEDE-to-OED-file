# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 14:30:13 2019

@author: kumar.shivam(kumar.shivam@xceedance.com)
"""

from mapping_dvc import mapping
from pre_process import pre_process
from logger import logging_process 
from outputwriter import output_write            
                 
def main():  
    """
    This is the main script..
    Calling all the instances here in this block.
    """       
    logger = logging_process()                                   
    AIR_location_file = pre_process().read_sql_data(logger)
    OED_location_file = pre_process().OED_file_preprocess(logger)
    OED_location_file_direct_mapped = mapping().direct_mapping(OED_location_file,AIR_location_file,logger)
    OED_location_file_value_mapped = mapping().value_mapping(OED_location_file_direct_mapped,AIR_location_file,logger)
    OED_location_file_final = mapping().conditional_mapping(OED_location_file_value_mapped,AIR_location_file,logger)
    output_write(OED_location_file_final,logger)

if __name__ == "__main__":
    main()

    
    

                            
                        
        
        





