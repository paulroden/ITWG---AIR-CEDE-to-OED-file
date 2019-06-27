# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 14:40:59 2019

@author: kumar.shivam
"""
import sys
import json


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
        try:
            with open(r"..\augmentations\location_direct_mapping.json") as json_file:  
                self.location_direct_mapping = json.load(json_file)
            logger.info('Successfully loaded direct mapping json file')                  
        except Exception as e:
            logger.info('Issue in loading direct mapping json file') 
            logger.error(e)
            print("Error Check Log file")
            sys.exit(0)
            
        try:
            for key in self.location_direct_mapping:  
                OED_location_file[key] = AIR_location_file[self.location_direct_mapping[key]] 
            OED_location_file_direct_mapped = OED_location_file
            logger.info('Successfully assign data in OED file as per direct mapping json file')                  
        except Exception as e:
            logger.info('Error in assigning data in OED file as per direct mapping json file') 
            logger.error(e)
            print("Error Check Log file")
            sys.exit(0)
        return OED_location_file_direct_mapped
        
    def value_mapping(self,OED_location_file_direct_mapped,AIR_location_file,logger):
        """
        After the direct mapping is done then are few columns which requires data to undergo values mapping.
        These columns include peril mapping, address match level mapping, construction code mapping,
        occupancy mapping and unit mapping.
        These value mappings are written in respective json file.
        These json files are used to map values for the mentioned columns.
        
        """
        try:
            with open(r"..\augmentations\peril_mapping.json") as json_file:  
                self.peril_mapping = json.load(json_file)
            logger.info('Successfully read peril value mapping data')                  
        except Exception as e:
            logger.info('Issue in reading peril value mapping data')
            logger.error(e)  
            print("Error Check Log file")   
            sys.exit(0)             
         
        try:
            with open(r"..\augmentations\address_match_mapping.json") as json_file:  
                self.address_match_mapping = json.load(json_file)
            logger.info('Successfully read address match value mapping data')                  
        except Exception as e:
            logger.info('Issue in reading address match value mapping data')
            logger.error(e)  
            print("Error Check Log file")
            sys.exit(0)
         
        try:
            with open(r"..\augmentations\construction_codes.json") as json_file:  
                self.construction_codes = json.load(json_file)
            logger.info('Successfully read construction code value mapping data')                  
        except Exception as e:
            logger.info('Issue in reading construction code value mapping data')
            logger.error(e)   
            print("Error Check Log file")
            sys.exit(0)
         
        try:
            with open(r"..\augmentations\occupancy_codes.json") as json_file:  
                self.occupancy_codes = json.load(json_file) 
            logger.info('Successfully read occupancy mapping code data')                  
        except Exception as e:
            logger.info('Issue in reading occupancy mapping code data')
            logger.error(e) 
            print("Error Check Log file")
            sys.exit(0)
        
        try:
            with open(r"..\augmentations\unit_mapping.json") as json_file:  
                self.unit_mapping = json.load(json_file)
            logger.info('Successfully read unit mapping code data')                  
        except Exception as e:
            logger.info('Issue in reading unit mapping code data')
            logger.error(e) 
            print("Error Check Log file")
            sys.exit(0)
         
        

        for index, row in OED_location_file_direct_mapped.iterrows():
            try:   
                OED_location_file_direct_mapped['LocPeril'] = OED_location_file_direct_mapped['LocPeril'].astype(str)
                OED_location_file_direct_mapped.at[index, 'LocPeril'] = self.peril_mapping['{}'.format(OED_location_file_direct_mapped.at[index, 'LocPeril'])] 
                logger.info('Successfully assigned peril value for LocPeril data')                  
            except Exception as e:
                logger.info('Issue in assigning peril value for LocPeril data')
                logger.error(e)   
                print("Error Check Log file")
                sys.exit(0)
                
            try:
                OED_location_file_direct_mapped['LocPerilsCovered'] = OED_location_file_direct_mapped['LocPerilsCovered'].astype(str)
                OED_location_file_direct_mapped.at[index, 'LocPerilsCovered'] = self.peril_mapping['{}'.format(OED_location_file_direct_mapped.at[index, 'LocPerilsCovered'])] 
                logger.info('Successfully assigned peril value for LocPerilsCovered data')                  
            except Exception as e:
                logger.info('Issue in assigning peril value for LocPerilsCovered data')
                logger.error(e) 
                print("Error Check Log file")    
                sys.exit(0)
                
            try:
                OED_location_file_direct_mapped.at[index, 'AddressMatch'] = self.address_match_mapping[OED_location_file_direct_mapped.at[index, 'AddressMatch']] 
                logger.info('Successfully assigned address match value for AddressMatch data')                  
            except Exception as e:
                logger.info('Issue in assigning address match value for AddressMatch data')
                logger.error(e) 
                print("Error Check Log file")
                sys.exit(0)
                
            try:
                OED_location_file_direct_mapped.at[index, 'OccupancyCode'] = self.occupancy_codes['{}'.format(OED_location_file_direct_mapped.at[index, 'OccupancyCode'])] 
                logger.info('Successfully assigning OccupancyCode value for OccupancyCode data')                  
            except Exception as e:
                logger.info('Issue in assigning OccupancyCode value for OccupancyCode data')
                logger.error(e) 
                print("Error Check Log file")
                sys.exit(0)
                                
            try:
                OED_location_file_direct_mapped.at[index, 'ConstructionCode'] = self.construction_codes['{}'.format(OED_location_file_direct_mapped.at[index, 'ConstructionCode'])] 
                logger.info('Successfully assigning ConstructionCode value for ConstructionCode data')                  
            except Exception as e:
                logger.info('Issue in assigning ConstructionCode value for ConstructionCode data')
                logger.error(e) 
                print("Error Check Log file")  
                sys.exit(0)
                
            try:
                OED_location_file_direct_mapped.at[index, 'FloorAreaUnit'] = self.unit_mapping['{}'.format(OED_location_file_direct_mapped.at[index, 'FloorAreaUnit'])] 
                OED_location_file_value_mapped = OED_location_file_direct_mapped
                logger.info('Successfully assigning UnitMapping value for UnitMapping data')                  
            except Exception as e:
                logger.info('Issue in assigning UnitMapping value for UnitMapping data')
                logger.error(e)
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
            logger.error(e)  
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
                    OED_location_file_value_mapped.at[index, 'LocDed6All'] = AIR_location_file['Deductible1'][index]+AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible3'][index]+AIR_location_file['Deductible4'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed6All'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'FR':
                    OED_location_file_value_mapped.at[index, 'LocDed6All'] = AIR_location_file['Deductible1'][index]+AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible3'][index]+AIR_location_file['Deductible4'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode6All'] = 2
                    OED_location_file_value_mapped.at[index, 'LocDedType6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed6All'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'PL':
                    OED_location_file_value_mapped.at[index, 'LocDed6All'] = AIR_location_file['Deductible1'][index]
                    OED_location_file_value_mapped.at[index, 'LocDedCode6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType6All'] = 1
                    OED_location_file_value_mapped.at[index, 'LocMinDed6All'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed6All'] = 0
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
                    OED_location_file_value_mapped.at[index, 'LocDedCode4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocDedType4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMinDed4BI'] = 0
                    OED_location_file_value_mapped.at[index, 'LocMaxDed4BI'] = 0
            logger.info('Successfully assigning Deductible term value as per DeductibleTypeCode condition')                  
        except Exception as e:
            logger.info('Issue in assigning Deductible term value as per DeductibleTypeCode condition')
            logger.error(e)
            print("Error Check Log file")
            sys.exit(0)
        
        try:        
            for index, row in OED_location_file_value_mapped.iterrows():
                OED_location_file_value_mapped.at[index,'CondPriority'] = 1
                OED_location_file_value_mapped.at[index,'LocParticipation'] = AIR_location_file['Participation2'][index]
            logger.info('Successfully assigning CondPriority, LocParticipation term value')                  
        except Exception as e:
            logger.info('Issue in assigning CondPriority, LocParticipation term value')
            logger.error(e)
            print("Error Check Log file")
            sys.exit(0)    
        OED_location_file_final = OED_location_file_value_mapped
        return OED_location_file_final