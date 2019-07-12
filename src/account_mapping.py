# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 12:19:45 2019

@author: kumar.shivam
"""
import sys 

class mapping_account:
    """
    The dataframe created from the CEDE AIR database has few columns which are directly mapped without any alteration
    to OED file. 
    
    """
    def conditional_mapping(self,OED_file_value_mapped, AIR_account_file,logger):
        """
        The coditional mapping is on limits, deductible type code.
        Assignment is done through if else statement.
        
        """
        try:
            for index, row in AIR_account_file.iterrows(): 
                if AIR_account_file['DeductibleTypeCode'][index] == 'N' and AIR_account_file['OccLimitTypeCode'][index] == 'B':
                    OED_file_value_mapped.at[index, 'CondLimit6All'] = AIR_account_file['OccLimit1'][index]
                    OED_file_value_mapped.at[index, 'CondDed6All'] = AIR_account_file['Attachment1'][index]
                
                elif AIR_account_file['DeductibleTypeCode'][index] == 'MI' and AIR_account_file['OccLimitTypeCode'][index] == 'B':
                    OED_file_value_mapped.at[index, 'CondLimit6All'] = AIR_account_file['OccLimit1'][index]
                    OED_file_value_mapped.at[index, 'CondDed6All'] = AIR_account_file['Attachment1'][index] 
                    OED_file_value_mapped.at[index, 'CondMinDed6All'] = AIR_account_file['Deductible1'][index]
                
                elif AIR_account_file['DeductibleTypeCode'][index] == 'MA' and AIR_account_file['OccLimitTypeCode'][index] == 'B':
                    OED_file_value_mapped.at[index, 'CondLimit6All'] = AIR_account_file['OccLimit1'][index]
                    OED_file_value_mapped.at[index, 'CondDed6All'] = AIR_account_file['Attachment1'][index] 
                    OED_file_value_mapped.at[index, 'CondMaxDed6All'] = AIR_account_file['Deductible2'][index] 
                
                elif AIR_account_file['DeductibleTypeCode'][index] == 'MM' and AIR_account_file['OccLimitTypeCode'][index] == 'B':
                    OED_file_value_mapped.at[index, 'CondLimit6All'] = AIR_account_file['OccLimit1'][index]
                    OED_file_value_mapped.at[index, 'CondDed6All'] = AIR_account_file['Attachment1'][index] 
                    OED_file_value_mapped.at[index, 'CondMinDed6All'] = AIR_account_file['Deductible1'][index]
                    OED_file_value_mapped.at[index, 'CondMaxDed6All'] = AIR_account_file['Deductible2'][index]            
                elif AIR_account_file['DeductibleTypeCode'][index] == 'N' and AIR_account_file['OccLimitTypeCode'][index] == 'E':
                    OED_file_value_mapped.at[index, 'CondLimit6All'] = AIR_account_file['OccLimit2'][index]
                    OED_file_value_mapped.at[index, 'CondDed6All'] = AIR_account_file['Attachment1'][index]
                elif AIR_account_file['DeductibleTypeCode'][index] == 'MI' and AIR_account_file['OccLimitTypeCode'][index] == 'E':
                    OED_file_value_mapped.at[index, 'CondLimit6All'] = AIR_account_file['OccLimit2'][index]
                    OED_file_value_mapped.at[index, 'CondDed6All'] = AIR_account_file['Attachment1'][index] 
                    OED_file_value_mapped.at[index, 'CondMinDed6All'] = AIR_account_file['Deductible1'][index]
                elif AIR_account_file['DeductibleTypeCode'][index] == 'MA' and AIR_account_file['OccLimitTypeCode'][index] == 'E':
                    OED_file_value_mapped.at[index, 'CondLimit6All'] = AIR_account_file['OccLimit2'][index]
                    OED_file_value_mapped.at[index, 'CondDed6All'] = AIR_account_file['Attachment1'][index] 
                    OED_file_value_mapped.at[index, 'CondMaxDed6All'] = AIR_account_file['Deductible2'][index]
                elif AIR_account_file['DeductibleTypeCode'][index] == 'MM' and AIR_account_file['OccLimitTypeCode'][index] == 'E':
                    OED_file_value_mapped.at[index, 'CondLimit6All'] = AIR_account_file['OccLimit2'][index]
                    OED_file_value_mapped.at[index, 'CondDed6All'] = AIR_account_file['Attachment1'][index] 
                    OED_file_value_mapped.at[index, 'CondMinDed6All'] = AIR_account_file['Deductible1'][index]
                    OED_file_value_mapped.at[index, 'CondMaxDed6All'] = AIR_account_file['Deductible2'][index]
                elif AIR_account_file['DeductibleTypeCode'][index] == 'N' and AIR_account_file['OccLimitTypeCode'][index] == 'CB':
                    OED_file_value_mapped.at[index, 'CondLimit5PD'] = AIR_account_file['OccLimit1'][index]
                    OED_file_value_mapped.at[index, 'CondDed5PD'] = AIR_account_file['Attachment1'][index] 
                    OED_file_value_mapped.at[index, 'CondLimit4BI'] = AIR_account_file['OccLimit4'][index]
                    OED_file_value_mapped.at[index, 'CondDed4BI'] = AIR_account_file['Attachment4'][index]
                elif AIR_account_file['DeductibleTypeCode'][index] == 'N' and AIR_account_file['OccLimitTypeCode'][index] == 'C':
                    OED_file_value_mapped.at[index, 'CondLimit1Building'] = AIR_account_file['OccLimit1'][index]
                    OED_file_value_mapped.at[index, 'CondDed1Building'] = AIR_account_file['Attachment1'][index] 
                    OED_file_value_mapped.at[index, 'CondLimit2Other'] = AIR_account_file['OccLimit2'][index]
                    OED_file_value_mapped.at[index, 'CondDed2Other'] = AIR_account_file['Attachment2'][index]
                    OED_file_value_mapped.at[index, 'CondLimit3Contents'] = AIR_account_file['OccLimit3'][index]
                    OED_file_value_mapped.at[index, 'CondDed3Contents'] = AIR_account_file['Attachment3'][index] 
                    OED_file_value_mapped.at[index, 'CondLimit4BI'] = AIR_account_file['OccLimit4'][index]
                    OED_file_value_mapped.at[index, 'CondDed4BI'] = AIR_account_file['Attachment4'][index]
              
              
                if AIR_account_file['OccLimitTypeCodeTL'][index] == 'E':
                    OED_file_value_mapped.at[index, 'LayerParticipation'] = AIR_account_file['OccParticipation'][index]
                    OED_file_value_mapped.at[index, 'LayerLimit'] = AIR_account_file['OccTotalLimit'][index]
                    if AIR_account_file['AttachmentPoint'][index] == 0 and AIR_account_file['DeductibleTypeCodeTL'][index] == 'AP' and AIR_account_file['Deductible1TL'] > 0:
                        OED_file_value_mapped.at[index, 'LayerAttachment'] = AIR_account_file['Deductible1TL'][index]
                    else:
                        OED_file_value_mapped.at[index, 'LayerAttachment'] = AIR_account_file['AttachmentPoint'][index]
                elif AIR_account_file['OccLimitTypeCodeTL'][index] == 'B':
                    OED_file_value_mapped.at[index, 'LayerLimit'] = AIR_account_file['OccTotalLimit'][index]
                elif AIR_account_file['OccLimitTypeCodeTL'][index] == 'N':
                    OED_file_value_mapped.at[index, 'LayerLimit'] = 0
                    
                                                
                if AIR_account_file['DeductibleTypeCodeTL'][index] == 'B':
                    OED_file_value_mapped.at[index, 'PolDed6All'] = AIR_account_file['Deductible1TL'][index]
                elif AIR_account_file['DeductibleTypeCodeTL'][index] == 'FR':
                    OED_file_value_mapped.at[index, 'PolDed6All'] = AIR_account_file['Deductible1TL'][index]
                    OED_file_value_mapped.at[index, 'PolDedCode6All'] = 2
                elif AIR_account_file['DeductibleTypeCodeTL'][index] == 'PL':
                    OED_file_value_mapped.at[index, 'PolDed6All'] = AIR_account_file['Deductible1TL'][index]
                    OED_file_value_mapped.at[index, 'PolDedType6All'] = 1
                elif AIR_account_file['DeductibleTypeCodeTL'][index] == 'MI':
                    OED_file_value_mapped.at[index, 'PolMinDed6All'] = AIR_account_file['Deductible1TL'][index]
                elif AIR_account_file['DeductibleTypeCodeTL'][index] == 'MA':
                    OED_file_value_mapped.at[index, 'PolMaxDed6All'] = AIR_account_file['Deductible2TL'][index]
                elif AIR_account_file['DeductibleTypeCodeTL'][index] == 'MM':
                    OED_file_value_mapped.at[index, 'PolMinDed6All'] = AIR_account_file['Deductible1TL'][index] 
                    OED_file_value_mapped.at[index, 'PolMaxDed6All'] = AIR_account_file['Deductible2TL'][index]
            logger.info('Successfully done conditional mapping for account file.')                  
        except Exception as e:
            logger.info('Issue in conditional mapping for account file.')
            logger.error(e,exc_info=True)
            print("Error Check Log file")
            sys.exit(0)
        return OED_file_value_mapped   
                    