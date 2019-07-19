# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 14:40:59 2019

@author: kumar.shivam
"""
import sys
from generic_mapping import genericmapping
from constants import constants

class mapping:
    """
    The dataframe created from the CEDE AIR database has few columns which are directly mapped without any alteration
    to OED file. This direct mapping is stored in a pre-written json file.
    
    """
    def direct_mapping(self,OED_location_file,AIR_location_file,logger):
        """
        The direct mapping json file is read and used as a dictionary with key value 
        and based on that the AIR data column in written to OED file using corresponding key
        values.
        """
        OED_location_file_direct_mapped = genericmapping().direct_mapping(OED_location_file, AIR_location_file, constants.LOCATION_DIRECT_MAPPING_JSON, logger)
        OED_location_file_direct_mapped['PercentComplete'] = OED_location_file_direct_mapped['PercentComplete'].replace(0.0,None)
        return OED_location_file_direct_mapped
        
    def value_mapping(self,OED_location_file_direct_mapped,AIR_location_file,logger):
        """
        After the direct mapping is done then are few columns which requires data to undergo values mapping.
        These columns include peril mapping, address match level mapping, construction code mapping,
        occupancy mapping and unit mapping.
        These value mappings are written in respective json file.
        These json files are used to map values for the mentioned columns.
        
        """           
        OED_location_file_direct_mapped = genericmapping().peril_mapping(OED_location_file_direct_mapped,AIR_location_file,constants.OED_LOC_PERIL_COL,logger)
        OED_location_file_direct_mapped = genericmapping().peril_mapping(OED_location_file_direct_mapped,AIR_location_file,constants.OED_LOC_PERIL_COV_COL,logger)      
        OED_location_file_value_mapped = genericmapping().value_mapper(constants.ADRRESS_MATCH_MAPPING,OED_location_file_direct_mapped,constants.ADDRESSMATCH_COL,True,logger)   
        OED_location_file_value_mapped = genericmapping().value_mapper(constants.OCCUPANCY_CODE_MAPPING,OED_location_file_direct_mapped,constants.OCCUPANCY_COL,False,logger)   
        OED_location_file_value_mapped = genericmapping().value_mapper(constants.CONSTRUCTION_CODE_MAPPING,OED_location_file_direct_mapped,constants.CONSTRUCTION_COL,False,logger)   
        OED_location_file_value_mapped = genericmapping().value_mapper(constants.UNIT_MAPPING,OED_location_file_direct_mapped,constants.FLOORAREA_COL,False,logger) 
        for index, row in OED_location_file_value_mapped.iterrows():
            try:
                OED_location_file_value_mapped.at[index, 'GeogScheme1'] = "XSUBA"   
                OED_location_file_value_mapped.at[index, 'GeogScheme2'] = "XSUB2"
                logger.info('Successfully assigning Geographic scheme with constant')                  
            except Exception as e:
                logger.info('Issue in assigning Geographic scheme with constant')
                logger.error(e,exc_info=True)
                print("Error Check Log file")
                sys.exit(0)
        return OED_location_file_value_mapped    

    def conditional_mapping(self,OED_location_file_value_mapped, AIR_location_file,logger):
        """
        There are columns which are related financial terms and condition.
        These columns are based on LimitTypeCode and DeductibleTypeCode.
        The conditional mapping are done on basis of these two columns with condditions.
        """
        try:
            for index, row in AIR_location_file.iterrows(): 
                if AIR_location_file['LimitTypeCode'][index] == 'S':
                    OED_location_file_value_mapped.at[index, 'LocLimitCode6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocLimitType6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocLimit6All'] = AIR_location_file['Limit1'][index]
                elif AIR_location_file['LimitTypeCode'][index] == 'C':
                    OED_location_file_value_mapped.at[index, 'LocLimitCode1Building'] = 0
                    OED_location_file_value_mapped.at[index, 'LocLimitType1Building'] = 0
                    OED_location_file_value_mapped.at[index, 'LocLimit1Building'] = AIR_location_file['Limit1'][index]
                    OED_location_file_value_mapped.at[index, 'LocLimitCode2Other'] = 0
                    OED_location_file_value_mapped.at[index, 'LocLimitType2Other'] = 0
                    OED_location_file_value_mapped.at[index, 'LocLimit2Other'] = AIR_location_file['Limit2'][index]
                    OED_location_file_value_mapped.at[index, 'LocLimitCode3Contents'] = 0
                    OED_location_file_value_mapped.at[index, 'LocLimitType3Contents'] = 0
                    OED_location_file_value_mapped.at[index, 'LocLimit3Contents'] = AIR_location_file['Limit3'][index]
                    OED_location_file_value_mapped.at[index, 'LocLimitCode4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocLimitType4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocLimit4BI'] = AIR_location_file['Limit4'][index]
            logger.info('Successfully assigning Limit term value as per LimitTypeCode condition')                  
        except Exception as e:
            logger.info('Issue in assigning Limit term value as per LimitTypeCode condition')
            logger.error(e,exc_info=True)  
            print("Error Check Log file")
            sys.exit(0)
        
        try:
            for index, row in AIR_location_file.iterrows():  
                if AIR_location_file['DeductibleTypeCode'][index] == 'N':
                    OED_location_file_value_mapped.at[index, 'LocDedCode6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed6All'] = 0   
                elif AIR_location_file['DeductibleTypeCode'][index] == 'C':
                    OED_location_file_value_mapped.at[index, 'LocDed1Building'] = AIR_location_file['Deductible1'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode1Building'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType1Building'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed1Building'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed1Building'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDed2Other'] = AIR_location_file['Deductible2'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode2Other'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType2Other'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed2Other'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed2Other'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDed3Contents'] = AIR_location_file['Deductible3'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode3Contents'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType3Contents'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed3Contents'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed3Contents'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDed4BI'] = AIR_location_file['Deductible4'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed4BI'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'CB':
                    OED_location_file_value_mapped.at[index, 'LocDed5PD'] = AIR_location_file['Deductible1'][index]+AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible3'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode5PD'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType5PD'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed5PD'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed5PD'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'CT':
                    OED_location_file_value_mapped.at[index, 'LocDed5PD'] = AIR_location_file['Deductible1'][index]+AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible3'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode5PD'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType5PD'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed5PD'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed5PD'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDed4BI'] = AIR_location_file['Deductible4'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed4BI'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'S':   
                    self.Ded1 = AIR_location_file['Deductible1'][index]
                    self.Ded2 = AIR_location_file['Deductible2'][index]
                    self.Ded3 = AIR_location_file['Deductible3'][index]
                    self.Ded4 = AIR_location_file['Deductible4'][index]
                    if self.Ded1 < 1:
                        self.Ded1 = self.Ded1 * AIR_location_file['Limit1'][index]
                    if self.Ded2 < 1:
                        self.Ded2 = self.Ded2 * AIR_location_file['Limit2'][index]
                    if self.Ded3 < 1:
                        self.Ded3 = self.Ded3 * AIR_location_file['Limit3'][index]
                    if self.Ded4 < 1:
                        self.Ded4 = self.Ded4 * AIR_location_file['Limit4'][index]
                    OED_location_file_value_mapped.at[index, 'LocDed6All'] = self.Ded1+self.Ded2+self.Ded3+self.Ded4
                    OED_location_file_value_mapped.at[index, 'LocDedCode6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed6All'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'FR':
                    OED_location_file_value_mapped.at[index, 'LocDed6All'] = AIR_location_file['Deductible1'][index]+AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible3'][index]+AIR_location_file['Deductible4'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode6All'] = 1
                    OED_location_file_value_mapped.at[index, 'LocDedType6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed6All'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'PL':
                    OED_location_file_value_mapped.at[index, 'LocDed1Building'] = AIR_location_file['Deductible1'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedType1Building'] = 1                                                                                                          
                    OED_location_file_value_mapped.at[index, 'LocDed2Other'] = AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible3'][index]+AIR_location_file['Deductible4'][index]
                    OED_location_file_value_mapped.at[index, 'LocDeductType2Other'] = 1                                                                                                          
                    OED_location_file_value_mapped.at[index, 'LocDed3Content'] = AIR_location_file['Deductible3'][index]+AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible3'][index]+AIR_location_file['Deductible4'][index]
                    OED_location_file_value_mapped.at[index, 'LocDeductType3Content'] = 1                                                                                                         
                    OED_location_file_value_mapped.at[index, 'LocDed4BI'] = AIR_location_file['Deductible4'][index]+AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible3'][index]+AIR_location_file['Deductible4'][index]
                    OED_location_file_value_mapped.at[index, 'LocDeductType4BI'] = 1
                elif AIR_location_file['DeductibleTypeCode'][index] == 'MP':
                    OED_location_file_value_mapped.at[index, 'LocDed1Building'] = AIR_location_file['Deductible1'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode1Building'] = 5
                    OED_location_file_value_mapped.at[index, 'LocDedType1Building'] = 2
                    OED_location_file_value_mapped.at[index, 'LocMinDed1Building'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed1Building'] = 0                                                     
                    OED_location_file_value_mapped.at[index, 'LocDed2Other'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedCode2Other'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDeductType2Other'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed2Other'] = 0                                                            
                    OED_location_file_value_mapped.at[index, 'LocMaxDed2Other'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDed3Content'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedCode3Content'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDeductType3Content'] = 0                                                          
                    OED_location_file_value_mapped.at[index, 'LocMinDed3Content'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed3Content'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDed4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedCode4BI'] = 0                                                         
                    OED_location_file_value_mapped.at[index, 'LocDeductType4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed4BI'] = 0    
                elif AIR_location_file['DeductibleTypeCode'][index] == 'ML':
                    OED_location_file_value_mapped.at[index, 'LocDed5PD'] = AIR_location_file['Deductible2'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode5PD'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType5PD'] = 1
                    OED_location_file_value_mapped.at[index, 'LocMinDed5PD'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed5PD'] = AIR_location_file['Deductible1'][index]
                    OED_location_file_value_mapped.at[index, 'LocDed4BI'] = AIR_location_file['Deductible4'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed4BI'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'AA':
                    OED_location_file_value_mapped.at[index, 'LocDed5PD'] = AIR_location_file['Deductible1'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode5PD'] = 1
                    OED_location_file_value_mapped.at[index, 'LocDedType5PD'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed5PD'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed5PD'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDed4BI'] = AIR_location_file['Deductible4'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode4BI'] = 1
                    OED_location_file_value_mapped.at[index, 'LocDedType4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed4BI'] = 0
            logger.info('Successfully assigning Deductible term value as per DeductibleTypeCode condition')                  
        except Exception as e:
            logger.info('Issue in assigning Deductible term value as per DeductibleTypeCode condition')
            logger.error(e,exc_info=True)
            print("Error Check Log file")
            sys.exit(0)          
        try:        
            for index, row in OED_location_file_value_mapped.iterrows():
                OED_location_file_value_mapped.at[index,'CondPriority'] = 1
                OED_location_file_value_mapped.at[index,'FloodDefenseHeightUnit'] = 1
                OED_location_file_value_mapped.at[index,'LocParticipation'] = AIR_location_file['Participation2'][index] 
            logger.info('Successfully assigning CondPriority, LocParticipation term value')                  
        except Exception as e:
            logger.info('Issue in assigning CondPriority, LocParticipation term value')
            logger.error(e)
            print("Error Check Log file")
            sys.exit(0)    
        OED_location_file_final = OED_location_file_value_mapped
        return OED_location_file_final