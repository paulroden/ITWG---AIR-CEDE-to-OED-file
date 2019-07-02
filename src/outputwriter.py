# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 18:07:18 2019

@author: kumar.shivam
"""
import sys

"""
This will write an output csv in output folder in this directory.
"""   
def output_write(OED_location_file_final,logger):   
    try:
        OED_location_file_final.to_csv(r"..\output\OED_input.csv",index=False)   
        logger.info('Successfully written output file')                  
    except Exception as e:
        logger.info('Issue in writting file')
        logger.error(e)
        print("Error Check Log file")
        sys.exit(0)
    print("Succesfully written converted file in output folder")    