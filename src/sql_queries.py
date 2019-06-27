# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 18:54:52 2019

@author: kumar.shivam
"""

query_tLocTm_tLoc_tExSet_tLocFeat = "select tl.LocationSID,te.ExposureSetName, tl.LocationID, tl.LocationName, tl.LocationGroup, tl.IsPrimaryLocation,tl.IsTenant,\
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

query_tlclx_tlc = "select distinct tlclx.LocationSID as LSID1temp, tlc.AppliesToTag as 'CondName',dcondname_number.CondNumber as \
            'LayerConditionSID' from tLayerConditionLocationXref as tlclx with (Nolock) inner join tLayerCondition as tlc on \
            tlc.LayerConditionSID = tlclx.LayerConditionSID inner join  (SELECT distinct(AppliesToTag) as 'CondName',\
            DENSE_RANK() OVER (ORDER BY  AppliesToTag) AS CondNumber \
            FROM tLayerCondition) as dcondname_number on dcondname_number.CondName = tlc.AppliesToTag"
                                        
query_tLoc_tContr = 'select tc.ContractID,tc.PerilSetCode as ContractPerilSetCode from tLocTerm as tlt with (Nolock) inner join \
            tLocation as tl  on tl.LocationSID = tlt.LocationSID inner join tContract as tc \
            on tc.ContractID = tl.ContractSID' 