# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 12:30:41 2019

@author: Kumar Shivam(kumar.shivam@xceedance.com)
"""

import pandas as pd

def OED_AIR_contract_json():
    OED_AIR_contract_direct_mapping = pd.read_csv(r"..\augmentations\location_direct_mapping.csv",header = 0, index_col=0)
    OED_AIR_contract_direct_mapping.to_json('location_direct_mapping.json')


def OED_AIR_peril_mapper_json():
    OED_AIR_peril_mapping = pd.read_csv(r"..\augmentations\peril_mapping.csv",header = 0, index_col=0)
    OED_AIR_peril_mapping.to_json('peril_mapping.json')
    

def OED_AIR_occupancy_mapper_json():
    OED_AIR_peril_mapping = pd.read_csv(r"..\augmentations\occupancy_codes.csv",header = 0, index_col=0)
    OED_AIR_peril_mapping.to_json('occupancy_codes.json')
    
    
def OED_AIR_unit_mapper_json():
    OED_AIR_peril_mapping = pd.read_csv(r"..\augmentations\unit_mapping.csv",header = 0, index_col=0)
    OED_AIR_peril_mapping.to_json('unit_mapping.json')
    
def OED_AIR_construction_code_mapper_json():
    OED_AIR_peril_mapping = pd.read_csv(r"..\augmentations\construction_codes.csv",header = 0, index_col=0)
    OED_AIR_peril_mapping.to_json('construction_codes.json')
    

    
def OED_AIR_address_match_mapper_json():
    OED_AIR_peril_mapping = pd.read_csv(r"..\augmentations\address_match_mapping.csv",header = 0, index_col=0)
    OED_AIR_peril_mapping.to_json('address_match_mapping.json')    
    
    
    
OED_AIR_address_match_mapper_json()
    
