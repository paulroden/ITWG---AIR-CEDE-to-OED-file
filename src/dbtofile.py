# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 14:30:13 2019

@author: kumar.shivam
"""

import pyodbc
import pandas as pd
import json

sql_conn_AIR = pyodbc.connect(r'Driver={SQL Server};Server=BHLP-NDA-1177\SQLEXPRESS;Database=AIRExp_SampleData;Trusted_Connection=yes;')

query = "select tl.LocationSID,te.ExposureSetName, tl.LocationID, tl.LocationName, tl.LocationGroup, tl.IsPrimaryLocation,tl.IsTenant,\
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
tlt.DeductibleTypeCode,tlt.Deductible1,tlt.Deductible2,tlt.Deductible3,tlt.Deductible4  \
from tLocTerm as tlt inner join  tLocation  \
as tl on tl.LocationSID = tlt.LocationSID inner join tExposureSet as  \
te on te.ExposureSetSID = tl.ExposureSetSID full outer join tLocFeature as tlf on tlf.LocationSID = tl.LocationSID" 

AIR_location_file = pd.read_sql(query, sql_conn_AIR)

OED_location_column_names = pd.read_csv(r"..\augmentations\location_column_names.csv",header = 0, index_col=0)
OED_location_column_names_list = OED_location_column_names['ColumnNames'].tolist()
OED_location_file = pd.DataFrame(columns = OED_location_column_names_list)

with open(r"..\augmentations\location_direct_mapping.json") as json_file:  
    location_direct_mapping = json.load(json_file)

for key in location_direct_mapping:
    try:
        OED_location_file[key] = AIR_location_file[location_direct_mapping[key]]  
    except Exception as e:
        print(e)

with open(r"..\augmentations\peril_mapping.json") as json_file:  
    peril_mapping = json.load(json_file)
    
with open(r"..\augmentations\address_match_mapping.json") as json_file:  
    address_match_mapping = json.load(json_file)

with open(r"..\augmentations\construction_codes.json") as json_file:  
    construction_codes = json.load(json_file)

with open(r"..\augmentations\occupancy_codes.json") as json_file:  
    occupancy_codes = json.load(json_file)
    
with open(r"..\augmentations\unit_mapping.json") as json_file:  
    unit_mapping = json.load(json_file)


for index, row in OED_location_file.iterrows():
    try:   
        OED_location_file.at[index, 'LocPeril'] = peril_mapping['{}'.format(OED_location_file.at[index, 'LocPeril'])] 
    except Exception as e:
        print(e)        
    try:
        OED_location_file.at[index, 'LocPerilsCovered'] = peril_mapping['{}'.format(OED_location_file.at[index, 'LocPerilsCovered'])] 
    except Exception as e:
        print(e)
    try:
        OED_location_file.at[index, 'AddressMatch'] = address_match_mapping[OED_location_file.at[index, 'AddressMatch']] 
    except Exception as e:
        print(e)
    try:
        OED_location_file.at[index, 'OccupancyCode'] = occupancy_codes['{}'.format(OED_location_file.at[index, 'OccupancyCode'])] 
    except Exception as e:
        print(e)
    try:
        OED_location_file.at[index, 'ConstructionCode'] = construction_codes['{}'.format(OED_location_file.at[index, 'ConstructionCode'])] 
    except Exception as e:
        print(e)
    try:
        OED_location_file.at[index, 'FloorAreaUnit'] = unit_mapping['{}'.format(OED_location_file.at[index, 'FloorAreaUnit'])] 
    except Exception as e:
        print(e)
        
    
    
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
        OED_location_file.at[index, 'LocLimitCode3Content'] = 0
        OED_location_file.at[index, 'LocLimitType3Content'] = 0
        OED_location_file.at[index, 'LocLimit3Content'] = AIR_location_file['Limit3'][index]
        OED_location_file.at[index, 'LocLimitCode4BI'] = 0
        OED_location_file.at[index, 'LocLimitType4BI'] = 0
        OED_location_file.at[index, 'LocLimit4BI'] = AIR_location_file['Limit4'][index]
        
        
for index, row in AIR_location_file.iterrows(): 
    if AIR_location_file['DeductibleTypeCode'][index] == 'N':
        OED_location_file.at[index, 'LocDedCode6All'] = 0
        OED_location_file.at[index, 'LocDeductType6All'] = 0
        OED_location_file.at[index, 'LocMinDed6All'] = 0
        OED_location_file.at[index, 'LocMaxDed6All'] = 0   
    elif AIR_location_file['DeductibleTypeCode'][index] == 'C':
        OED_location_file.at[index, 'LocDed1Buildinig'] = AIR_location_file['Deductible1'][index]
        OED_location_file.at[index, 'LocDedCode1Building'] = 0
        OED_location_file.at[index, 'LocDedType1Building'] = 0
        OED_location_file.at[index, 'LocMinDed1Building'] = 0
        OED_location_file.at[index, 'LocMaxDed1Building'] = 0
        OED_location_file.at[index, 'LocDed2Other'] = AIR_location_file['Deductible2'][index]
        OED_location_file.at[index, 'LocDedCode2Other'] = 0
        OED_location_file.at[index, 'LocDeductType2Other'] = 0
        OED_location_file.at[index, 'LocMinDed2Other'] = 0
        OED_location_file.at[index, 'LocMaxDed2Other'] = 0
        OED_location_file.at[index, 'LocDed3Content'] = AIR_location_file['Deductible3'][index]
        OED_location_file.at[index, 'LocDedCode3Content'] = 0
        OED_location_file.at[index, 'LocDeductType3Content'] = 0
        OED_location_file.at[index, 'LocMinDed3Content'] = 0
        OED_location_file.at[index, 'LocMaxDed3Content'] = 0
        OED_location_file.at[index, 'LocDed4BI'] = AIR_location_file['Deductible4'][index]
        OED_location_file.at[index, 'LocDedCode4BI'] = 0
        OED_location_file.at[index, 'LocDeductType4BI'] = 0
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
                            
                        
        
        
OED_location_file.to_csv('OED_4column')   




