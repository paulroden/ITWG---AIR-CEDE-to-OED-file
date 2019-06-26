# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 14:30:13 2019

@author: kumar.shivam(kumar.shivam@xceedance.com)
"""

import pyodbc
import pandas as pd
import json
import logging
import sys

"""
Initiating logging in a file named conversion.log for each step
The logging would have error tags if encounter any error.
"""
try:
    logger = logging.getLogger('application')
    if logger.handlers:
        logger.handlers = []
    hdlr = logging.FileHandler('conversion.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.INFO)
    logger.info('Successfully initiated logging process')
except Exception as e:
    logger.info('Issue initiating logging process')
    logger.error(e)
    print("Error Check Log file")
    sys.exit(0)

class pre_process:
    """
    Preprocess includes connection to the SQL server, reading all required data from the AIR CEDE table.
    These are queries which joins data from multiple table to reflect into one dataframe.
    It also includes reading column names from pre-defined csv and creating black OED table.
    """
    def read_sql_data(self): 
        """
        Reading SQL data from table tLocTerm, tExposureSet, tLocation, tLocFeature, tLayerConditionLocationXref,
        tLocation and tContract.
        These data are joined as per rquirement shared in the documentation.
        """
        try:
            with open(r"..\augmentations\connection_string.json") as json_file:  
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
            self.query_tLocTm_tLoc_tExSet_tLocFeat = "select tl.LocationSID,te.ExposureSetName, tl.LocationID, tl.LocationName, tl.LocationGroup, tl.IsPrimaryLocation,tl.IsTenant,\
            tl.ISOBIN,tl.InceptionDate,tl.ExpirationDate,tl.CountryCode,tl.Latitude,tl.Longitude,tl.Address,tl.PostalCode,\
            tl.City,tl.AreaCode,tl.AreaName,tl.GeoMatchLevelCode,tl.GeocoderCode,tl.UserOccupancySchemeCode,tl.UserOccupancyCode,\
            tl.UserConstructionSchemeCode,tl.UserConstructionCodeA, tl.AIROccupancyCode, tl.AIRConstructionCodeA,\
            tl.YearBuilt,tl.Stories,tl.RiskCount,tl.GrossArea,tl.GrossAreaUnitCode,tl.UserDefined1,tl.UserDefined2,\
            tl.UserDefined3,tl.UserDefined3,tl.UserDefined4,tl.UserDefined5, tl.ReplacementValueA,tl.ReplacementValueB,\
            tl.ReplacementValueC,tl.ReplacementValueD,tl.BuildingHeight, tl.ReplacementValueDaysCovered,tl.CurrencyCode,tl.Premium,\
            tl.NonCATGroundUpLoss,tlt.PerilSetCode,tlt.Participation1,tlt.Participation2,tlf.ProjectCompletion, tl.BuildingHeightUnitCode,\
            tlf.RoofCoverCode,tlf.RoofYearBuilt,tlf.RoofGeometryCode,tlf.RoofAttachedStructureCode,tlf.RoofDeckCode,\
            tlf.RoofPitchCode,tlf.RoofAnchorageCode,tlf.RoofDeckAttachCode,tlf.RoofCoverAttachCode,tlf.GlassTypeCode,\
            tlf.LatticeCode,tlf.CustomFloodZoneCode,tlf.SoftStoryCode,tlf.BasementFinishTypeCode,tlf.BasementLevelCount,\
            tlf.WindowProtectionCode,tlf.FoundationCode,tlf.WallAttachedStructureCode,tlf.AppurtenantStructureCode,\
            tlf.IBHSFortifiedCode,tlf.EquipmentCode,tlf.BuildingShapeCode,tlf.ShapeIrregularityCode,tlf.PoundingCode,\
            tlf.OrnamentationCode,tlf.SpecialConstructionCode,tlf.RetrofitCode,tlf.FoundationConnectionCode, tlf.ShortColumnCode,\
            tlf.WallSidingCode,tlf.FirstFloorHeight,tlf.FirstFloorHeightUnitCode,tlf.CustomElevationUnitCode,\
            tlf.TankCode,tlf.RedundancyCode,tlf.InternalPartitionCode,tlf.ExternalDoorCode,tlf.TorsionCode,\
            tlf.ContentVulnerabilityCode,tlf.SmallDebrisCode,tlf.FloorsOccupied,tlf.BaseFloodElevation,\
            tlf.BaseFloodElevationUnitCode,tlf.TreeExposureCode,tlf.ChimneyCode,tlf.BrickVeneerCode,tlf.FIRMComplianceCode,\
            tlf.CustomFloodStandardOfProtection,tlf.CustomFloodZoneCode,tlf.MultiStoryHallCode,tlf.BuildingExteriorOpeningCode,\
            tlf.ServiceEquipmentProtectionCode,tlf.TallOneStoryCode,tlf.TerrainRoughnessCode,tlt.LimitTypeCode, \
            tlf.CustomElevation,tlf.BuildingConditionCode,tlt.Limit1,tlt.Limit2,tlt.Limit3,tlt.Limit4,\
            tlt.DeductibleTypeCode,tlt.Deductible1,tlt.Deductible2,tlt.Deductible3,tlt.Deductible4 \
            from tLocTerm as tlt with (Nolock) inner join tLocation  \
            as tl on tl.LocationSID = tlt.LocationSID inner join tExposureSet as  \
            te on te.ExposureSetSID = tl.ExposureSetSID full outer join tLocFeature as tlf on tlf.LocationSID = tl.LocationSID"                    
            global AIR_location_file           
            AIR_location_file = pd.read_sql(self.query_tLocTm_tLoc_tExSet_tLocFeat, self.sql_conn_AIR) 
            logger.info('Successfully read data from AIR DB for Tlocterm, Tloc, Texpset, tlocFeat')
        except Exception as e:
            logger.info('Issue in reading data from AIR DB for Tlocterm, Tloc, Texpset, tlocFeat')
            logger.error(e)
            print("Error Check Log file")
            sys.exit(0)

          
        try:    
            self.query_tLoc_tContr = 'select tc.ContractID from tLocTerm as tlt with (Nolock) inner join \
            tLocation as tl  on tl.LocationSID = tlt.LocationSID inner join tContract as tc \
            on tc.ContractID = tl.ContractSID'        
            ContractID  = pd.read_sql(self.query_tLoc_tContr, self.sql_conn_AIR)
            AIR_location_file = AIR_location_file.join(ContractID)  
            logger.info('Successfully read data from AIR DB for ContractID from tloc, tcontract')                  
        except Exception as e:   
            logger.info('Issue in reading data from AIR DB for ContractID from tloc, tcontract')   
            logger.error(e)   
            print("Error Check Log file") 
            sys.exit(0)              
               

        
        try:
            self.query_tlclx_tlc = "select distinct tlclx.LocationSID as LSID1temp, tlc.AppliesToTag as 'CondName',dcondname_number.CondNumber as \
            'LayerConditionSID' from tLayerConditionLocationXref as tlclx with (Nolock) inner join tLayerCondition as tlc on \
            tlc.LayerConditionSID = tlclx.LayerConditionSID inner join  (SELECT distinct(AppliesToTag) as 'CondName',\
            DENSE_RANK() OVER (ORDER BY  AppliesToTag) AS CondNumber \
            FROM tLayerCondition) as dcondname_number on dcondname_number.CondName = tlc.AppliesToTag"
            LayerConditionSID  = pd.read_sql(self.query_tlclx_tlc, self.sql_conn_AIR)
            AIR_location_file = AIR_location_file.join(LayerConditionSID) 
            self.sql_conn_AIR.close()
            logger.info('Successfully read data from AIR DB for LayerconditionSID. CondNumber from tlocCondXref, tLayerCondition')                  
        except Exception as e:   
            logger.info('Issue in reading data from AIR DB for LayerconditionSID. CondNumber from tlocCondXref, tLayerCondition')                  
            logger.error(e) 
            print("Error Check Log file")
            sys.exit(0)

    
    def OED_file_preprocess(self):
        """
        Reading the column names required for the OED files from location column names csv and creating
        the empty dataframe with the same column names,
        """
        try:
            global OED_location_file
            self.OED_location_column_names = pd.read_csv(r"..\augmentations\location_column_names.csv",header = 0, index_col=0)
            self.OED_location_column_names_list = self.OED_location_column_names['ColumnNames'].tolist()            
            OED_location_file = pd.DataFrame(columns = self.OED_location_column_names_list)
            logger.info('Successfully created blank OED file')                  
        except Exception as e:
            logger.info('Issue in creating blank OED file') 
            logger.error(e)  
            print("Error Check Log file") 
            sys.exit(0)              

class mapping:
    """
    The dataframe created from the CEDE AIR database has few columns which are directly mapped without any alteration
    to OED file. This direct mapping is stored in a pre-written json file.
    
    """
    def direct_mapping(self):
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
            logger.info('Successfully assign data in OED file as per direct mapping json file')                  
        except Exception as e:
            logger.info('Error in assigning data in OED file as per direct mapping json file') 
            logger.error(e)
            print("Error Check Log file")
            sys.exit(0)
       
    def value_mapping(self):
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
         
                    
        OED_location_file['LocPeril'] = OED_location_file['LocPeril'].astype(str)
        OED_location_file['LocPerilsCovered'] = OED_location_file['LocPerilsCovered'].astype(str)
        
        for index, row in OED_location_file.iterrows():
            try:   
                OED_location_file.at[index, 'LocPeril'] = self.peril_mapping['{}'.format(OED_location_file.at[index, 'LocPeril'])] 
                logger.info('Successfully assigned peril value for LocPeril data')                  
            except Exception as e:
                logger.info('Issue in assigning peril value for LocPeril data')
                logger.error(e)   
                print("Error Check Log file")
                sys.exit(0)
                
            try:
                OED_location_file.at[index, 'LocPerilsCovered'] = self.peril_mapping['{}'.format(OED_location_file.at[index, 'LocPerilsCovered'])] 
                logger.info('Successfully assigned peril value for LocPerilsCovered data')                  
            except Exception as e:
                logger.info('Issue in assigning peril value for LocPerilsCovered data')
                logger.error(e) 
                print("Error Check Log file")    
                sys.exit(0)
                
            try:
                OED_location_file.at[index, 'AddressMatch'] = self.address_match_mapping[OED_location_file.at[index, 'AddressMatch']] 
                logger.info('Successfully assigned address match value for AddressMatch data')                  
            except Exception as e:
                logger.info('Issue in assigning address match value for AddressMatch data')
                logger.error(e) 
                print("Error Check Log file")
                sys.exit(0)
                
            try:
                OED_location_file.at[index, 'OccupancyCode'] = self.occupancy_codes['{}'.format(OED_location_file.at[index, 'OccupancyCode'])] 
                logger.info('Successfully assigning OccupancyCode value for OccupancyCode data')                  
            except Exception as e:
                logger.info('Issue in assigning OccupancyCode value for OccupancyCode data')
                logger.error(e) 
                print("Error Check Log file")
                sys.exit(0)
                                
            try:
                OED_location_file.at[index, 'ConstructionCode'] = self.construction_codes['{}'.format(OED_location_file.at[index, 'ConstructionCode'])] 
                logger.info('Successfully assigning ConstructionCode value for ConstructionCode data')                  
            except Exception as e:
                logger.info('Issue in assigning ConstructionCode value for ConstructionCode data')
                logger.error(e) 
                print("Error Check Log file")  
                sys.exit(0)
                
            try:
                OED_location_file.at[index, 'FloorAreaUnit'] = self.unit_mapping['{}'.format(OED_location_file.at[index, 'FloorAreaUnit'])] 
                logger.info('Successfully assigning UnitMapping value for UnitMapping data')                  
            except Exception as e:
                logger.info('Issue in assigning UnitMapping value for UnitMapping data')
                logger.error(e)
                print("Error Check Log file")
                sys.exit(0)
                

    def conditional_mapping(self):
        """
        There are columns which are related financial terms and condition.
        These columns are based on LimitTypeCode and DeductibleTypeCode.
        The conditional mapping are done on basis of these two columns with condditions.
        """
        try:
            for index, row in AIR_location_file.iterrows(): 
                if AIR_location_file['LimitTypeCode'][index] == 'S':
                    OED_location_file.at[index, 'LocLimitCode6All'] = 0
                    OED_location_file.at[index, 'LocLimitType6All'] = 0
                    OED_location_file.at[index, 'LocLimit6All'] = AIR_location_file['Limit1'][index]
                elif AIR_location_file['LimitTypeCode'][index] == 'C':
                    OED_location_file.at[index, 'LocLimitCode1Building'] = 0
                    OED_location_file.at[index, 'LocLimitType1Building'] = 0
                    OED_location_file.at[index, 'LocLimit1Building'] = AIR_location_file['Limit1'][index]
                    OED_location_file.at[index, 'LocLimitCode2Other'] = 0
                    OED_location_file.at[index, 'LocLimitType2Other'] = 0
                    OED_location_file.at[index, 'LocLimit2Other'] = AIR_location_file['Limit2'][index]
                    OED_location_file.at[index, 'LocLimitCode3Contents'] = 0
                    OED_location_file.at[index, 'LocLimitType3Contents'] = 0
                    OED_location_file.at[index, 'LocLimit3Contents'] = AIR_location_file['Limit3'][index]
                    OED_location_file.at[index, 'LocLimitCode4BI'] = 0
                    OED_location_file.at[index, 'LocLimitType4BI'] = 0
                    OED_location_file.at[index, 'LocLimit4BI'] = AIR_location_file['Limit4'][index]
            logger.info('Successfully assigning Limit term value as per LimitTypeCode condition')                  
        except Exception as e:
            logger.info('Issue in assigning Limit term value as per LimitTypeCode condition')
            logger.error(e)  
            print("Error Check Log file")
            sys.exit(0)
        
        try:
            for index, row in AIR_location_file.iterrows():  
                if AIR_location_file['DeductibleTypeCode'][index] == 'N':
                    OED_location_file.at[index, 'LocDedCode6All'] = 0
                    OED_location_file.at[index, 'LocDedType6All'] = 0
                    OED_location_file.at[index, 'LocMinDed6All'] = 0
                    OED_location_file.at[index, 'LocMaxDed6All'] = 0   
                elif AIR_location_file['DeductibleTypeCode'][index] == 'C':
                    OED_location_file.at[index, 'LocDed1Building'] = AIR_location_file['Deductible1'][index]
                    OED_location_file.at[index, 'LocDedCode1Building'] = 0
                    OED_location_file.at[index, 'LocDedType1Building'] = 0
                    OED_location_file.at[index, 'LocMinDed1Building'] = 0
                    OED_location_file.at[index, 'LocMaxDed1Building'] = 0
                    OED_location_file.at[index, 'LocDed2Other'] = AIR_location_file['Deductible2'][index]
                    OED_location_file.at[index, 'LocDedCode2Other'] = 0
                    OED_location_file.at[index, 'LocDedType2Other'] = 0
                    OED_location_file.at[index, 'LocMinDed2Other'] = 0
                    OED_location_file.at[index, 'LocMaxDed2Other'] = 0
                    OED_location_file.at[index, 'LocDed3Contents'] = AIR_location_file['Deductible3'][index]
                    OED_location_file.at[index, 'LocDedCode3Contents'] = 0
                    OED_location_file.at[index, 'LocDedType3Contents'] = 0
                    OED_location_file.at[index, 'LocMinDed3Contents'] = 0
                    OED_location_file.at[index, 'LocMaxDed3Contents'] = 0
                    OED_location_file.at[index, 'LocDed4BI'] = AIR_location_file['Deductible4'][index]
                    OED_location_file.at[index, 'LocDedCode4BI'] = 0
                    OED_location_file.at[index, 'LocDedType4BI'] = 0
                    OED_location_file.at[index, 'LocMinDed4BI'] = 0
                    OED_location_file.at[index, 'LocMaxDed4BI'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'CB':
                    OED_location_file.at[index, 'LocDed5PD'] = AIR_location_file['Deductible1'][index]+AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible3'][index]
                    OED_location_file.at[index, 'LocDedCode5PD'] = 0
                    OED_location_file.at[index, 'LocDedType5PD'] = 0
                    OED_location_file.at[index, 'LocMinDed5PD'] = 0
                    OED_location_file.at[index, 'LocMaxDed5PD'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'CT':
                    OED_location_file.at[index, 'LocDed5PD'] = AIR_location_file['Deductible1'][index]+AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible3'][index]
                    OED_location_file.at[index, 'LocDedCode5PD'] = 0
                    OED_location_file.at[index, 'LocDedType5PD'] = 0
                    OED_location_file.at[index, 'LocMinDed5PD'] = 0
                    OED_location_file.at[index, 'LocMaxDed5PD'] = 0
                    OED_location_file.at[index, 'LocDed4BI'] = AIR_location_file['Deductible4'][index]
                    OED_location_file.at[index, 'LocDedCode4BI'] = 0
                    OED_location_file.at[index, 'LocDedType4BI'] = 0
                    OED_location_file.at[index, 'LocMinDed4BI'] = 0
                    OED_location_file.at[index, 'LocMaxDed4BI'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'S':
                    OED_location_file.at[index, 'LocDed6All'] = AIR_location_file['Deductible1'][index]+AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible3'][index]+AIR_location_file['Deductible4'][index]
                    OED_location_file.at[index, 'LocDedCode6All'] = 0
                    OED_location_file.at[index, 'LocDedType6All'] = 0
                    OED_location_file.at[index, 'LocMinDed6All'] = 0
                    OED_location_file.at[index, 'LocMaxDed6All'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'FR':
                    OED_location_file.at[index, 'LocDed6All'] = AIR_location_file['Deductible1'][index]+AIR_location_file['Deductible2'][index]+AIR_location_file['Deductible3'][index]+AIR_location_file['Deductible4'][index]
                    OED_location_file.at[index, 'LocDedCode6All'] = 2
                    OED_location_file.at[index, 'LocDedType6All'] = 0
                    OED_location_file.at[index, 'LocMinDed6All'] = 0
                    OED_location_file.at[index, 'LocMaxDed6All'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'PL':
                    OED_location_file.at[index, 'LocDed6All'] = AIR_location_file['Deductible1'][index]
                    OED_location_file.at[index, 'LocDedCode6All'] = 0
                    OED_location_file.at[index, 'LocDedType6All'] = 1
                    OED_location_file.at[index, 'LocMinDed6All'] = 0
                    OED_location_file.at[index, 'LocMaxDed6All'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'ML':
                    OED_location_file.at[index, 'LocDed5PD'] = AIR_location_file['Deductible2'][index]
                    OED_location_file.at[index, 'LocDedCode5PD'] = 0
                    OED_location_file.at[index, 'LocDedType5PD'] = 1
                    OED_location_file.at[index, 'LocMinDed5PD'] = 0
                    OED_location_file.at[index, 'LocMaxDed5PD'] = AIR_location_file['Deductible1'][index]
                    OED_location_file.at[index, 'LocDed4BI'] = AIR_location_file['Deductible4'][index]
                    OED_location_file.at[index, 'LocDedCode4BI'] = 0
                    OED_location_file.at[index, 'LocDedType4BI'] = 0
                    OED_location_file.at[index, 'LocMinDed4BI'] = 0
                elif AIR_location_file['DeductibleTypeCode'][index] == 'AA':
                    OED_location_file.at[index, 'LocDed5PD'] = AIR_location_file['Deductible1'][index]
                    OED_location_file.at[index, 'LocDedCode5PD'] = 0
                    OED_location_file.at[index, 'LocDedType5PD'] = 0
                    OED_location_file.at[index, 'LocMinDed5PD'] = 0
                    OED_location_file.at[index, 'LocMaxDed5PD'] = 0
                    OED_location_file.at[index, 'LocDed4BI'] = AIR_location_file['Deductible4'][index]
                    OED_location_file.at[index, 'LocDedCode4BI'] = 0
                    OED_location_file.at[index, 'LocDedType4BI'] = 0
                    OED_location_file.at[index, 'LocMinDed4BI'] = 0
                    OED_location_file.at[index, 'LocMaxDed4BI'] = 0
            logger.info('Successfully assigning Deductible term value as per DeductibleTypeCode condition')                  
        except Exception as e:
            logger.info('Issue in assigning Deductible term value as per DeductibleTypeCode condition')
            logger.error(e)
            print("Error Check Log file")
            sys.exit(0)
        
        try:        
            for index, row in OED_location_file.iterrows():
                OED_location_file.at[index,'CondPriority'] = 1
                OED_location_file.at[index,'LocParticipation'] = AIR_location_file['Participation1'][index] * AIR_location_file['Participation2'][index]
            logger.info('Successfully assigning CondPriority, LocParticipation term value')                  
        except Exception as e:
            logger.info('Issue in assigning CondPriority, LocParticipation term value')
            logger.error(e)
            print("Error Check Log file")
            sys.exit(0)
             
                 
def main():  
    """
    Calling all the instances here in this block.
    This will write an output csv in output folder in this directory.
    """                                         
    pre_process().read_sql_data()
    pre_process().OED_file_preprocess()
    mapping().direct_mapping()
    mapping().value_mapping()
    mapping().conditional_mapping()
    try:
        OED_location_file.to_csv(r"..\output\OED_input.csv",index=False)   
        logger.info('Successfully written output file')                  
    except Exception as e:
        logger.info('Issue in writting file')
        logger.error(e)
        print("Error Check Log file")
        sys.exit(0)
    print("Succesfully written converted file in output folder")    

if __name__ == "__main__":
    main()

    
    

                            
                        
        
        





