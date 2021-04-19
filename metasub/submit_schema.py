"""
submit_schema - schema for converting master table to cog submission csv. 

### CHANGE LOG ### 
2021-04-19 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build - split from cogsub 
"""

from marshmallow import fields, Schema, validate, pre_load
import datetime 

collection_date_min = datetime.date(2020, 1, 1)
region_to_county = {
    "EAST MIDLANDS" :  "DERBYSHIRE|LEICESTERSHIRE|LINCOLNSHIRE|NORTHAMPTONSHIRE|NOTTINGHAMSHIRE|RUTLAND",
    "EAST OF ENGLAND" : "BEDFORDSHIRE|CAMBRIDGESHIRE|ESSEX|HERTFORDSHIRE|NORFOLK|SUFFOLK",
    "LONDON" : "GREATER_LONDON",
    "NORTH EAST" : "TYNE_AND_WEAR|DURHAM|NORTHUMBERLAND|NORTH_YORKSHIRE",
    "NORTH WEST" : "CHESHIRE_EAST|CHESHIRE_WEST_AND_CHESTER|CUMBRIA|GREATER_MANCHESTER|LANCASHIRE|MERSEYSIDE",
    "SOUTH EAST" : "BUCKINGHAMSHIRE|SUSSEX|HAMPSHIRE|ISLE_OF_WIGHT|KENT|OXFORDSHIRE|BERKSHIRE|SURREY",
    "SOUTH WEST" : "BRISTOL|CORNWALL|DORSET|DEVON|GLOUCESTERSHIRE|SOMERSET|WILTSHIRE",
    "WEST MIDLANDS" : "HEREFORDSHIRE|SHROPSHIRE|STAFFORDSHIRE|WARWICKSHIRE|WEST_MIDLANDS|WORCESTERSHIRE",
    "YORKSHIRE AND THE HUMBER": "WEST_YORKSHIRE|SOUTH_YORKSHIRE|NORTH_LINCOLNSHIRE"
}

class Cogmeta(Schema):

    central_sample_id = fields.Str(required=True)
    adm1 = fields.Str(missing='UK-ENG')
    collection_date = fields.Date(validate=lambda x: x > collection_date_min)
    received_date = fields.Date()
    source_age = fields.Integer(validate=validate.Range(min=0, max=110))
    source_sex = fields.Str(validate=validate.OneOf(["M", "F"]))
    adm2 = fields.Str()
    adm2_private = fields.Str()
    collecting_org = fields.Str()
    biosample_source_id = fields.Str()
    sample_type_collected = fields.Str()
    swab_site = fields.Str()
    is_surveillance = fields.Str(default='Y')
    is_icu_patient = fields.Str(validate=validate.OneOf(["Y", "N"]))
    sender_sample_id = fields.Str()
    region = fields.Str()
    collection_pillar = fields.Int()

    @pre_load
    def clean_up(self, in_data, **kwargs):
        for k,v in dict(in_data).items():
            if v in ['', 'to check',  '#VALUE!', '-'] :
                in_data.pop(k)        
            elif isinstance(v, str):
                    in_data[k] = v.strip().upper()
        if in_data.get('is_icu_patient') not in ['Y', 'N']:
            if in_data.get('is_icu_patient'):
                in_data.pop('is_icu_patient')     
        if in_data.get('collecting_org'):
            if in_data.get('collecting_org').startswith('HMP '):
                in_data['collecting_org'] = 'HMP'
            if in_data['collecting_org'] == 'HMP': 
                in_data["is_surveillance"] = 'N'
        if not in_data.get('sender_sample_id') and in_data.get('biosample_source_id'):
            in_data['sender_sample_id'] = in_data['biosample_source_id']
        # handle REACT samples
        if in_data.get('region') and not in_data.get('adm2'):
            in_data['adm2'] = in_data.get('region')
        return in_data

class CtMeta(Schema):
    ct_1_ct_value = fields.Float(validate=validate.Range(min=0, max=2000))
    ct_1_test_kit = fields.Str(validate=validate.OneOf(["ALTONA", "ABBOTT", "INHOUSE", "ROCHE", "AUSDIAGNOSTICS", "BOSPHORE", "SEEGENE", "BD", "XPERT", "QIASTAT", "ALINITY", "AMPLIDIAG"]))
    ct_1_test_platform = fields.Str(validate=validate.OneOf(["APPLIED_BIO_7500","ALTOSTAR_AM16", "ABBOTT_M2000", "ROCHE_FLOW", "ROCHE_COBAS", "ELITE_INGENIUS", "CEPHEID_XPERT", "QIASTAT_DX", "AUSDIAGNOSTICS", "ROCHE_LIGHTCYCLER", "QIAGEN_ROTORGENE", "INHOUSE" ,"ALTONA", "PANTHER", "SEEGENE_NIMBUS", "BD_MAX", "AMPLIDIAG_EASY"]))
    ct_1_test_target = fields.Str(validate=validate.OneOf(["S", "E", "RDRP", "N", "ORF1AB", "ORF8", "RDRP+N"]))    
    ct_2_ct_value = fields.Float(validate=validate.Range(min=0, max=2000))
    ct_2_test_kit = fields.Str(validate=validate.OneOf(["ALTONA", "ABBOTT", "INHOUSE", "ROCHE", "AUSDIAGNOSTICS", "BOSPHORE", "SEEGENE", "BD", "XPERT", "QIASTAT", "ALINITY", "AMPLIDIAG"]))
    ct_2_test_platform = fields.Str(validate=validate.OneOf(["APPLIED_BIO_7500","ALTOSTAR_AM16", "ABBOTT_M2000", "ROCHE_FLOW", "ROCHE_COBAS", "ELITE_INGENIUS", "CEPHEID_XPERT", "QIASTAT_DX", "AUSDIAGNOSTICS", "ROCHE_LIGHTCYCLER", "QIAGEN_ROTORGENE", "INHOUSE" ,"ALTONA", "PANTHER", "SEEGENE_NIMBUS", "BD_MAX", "AMPLIDIAG_EASY"]))
    ct_2_test_target = fields.Str(validate=validate.OneOf(["S", "E", "RDRP", "N", "ORF1AB", "ORF8", "RDRP+N"]))    

    @pre_load
    def clean_up(self, in_data, **kwargs):
        for k,v in dict(in_data).items():
            if v in ['', 'to check',  '#VALUE!', '-'] :
                in_data.pop(k)        
            elif isinstance(v, str):
                    in_data[k] = v.strip().upper()
        return in_data

class RunMeta(Schema):
    run_name = fields.Str(required=True)
    instrument_make = fields.Str(default='ILLUMINA')
    instrument_model = fields.Str(default='NextSeq 500')
    bioinfo_pipe_name = fields.Str()
    bioinfo_pipe_version = fields.Str()

class LibraryHeaderMeta(Schema):
    library_name = fields.Str(required=True)
    library_seq_kit = fields.Str(default='Nextera')
    library_seq_protocol = fields.Str(default='Nextera LITE')
    library_layout_config = fields.Str(default='PAIRED')

class LibraryBiosampleMeta(Schema):
    central_sample_id = fields.Str(required=True)
    library_selection = fields.Str(default='PCR')
    library_source = fields.Str(default='VIRAL_RNA')
    library_strategy = fields.Str(default='AMPLICON')
    library_primers = fields.Integer(default=3)
    library_protocol = fields.Str(default='ARTICv2')
    sequencing_org_received_date = fields.Str()

    @pre_load
    def clean_up(self, in_data, **kwargs):
        for k,v in dict(in_data).items():
            if v in ['', 'to check',  '#VALUE!', '-'] :
                in_data.pop(k)        
            elif isinstance(v, str):
                    in_data[k] = v.strip().upper()
        if in_data.get('received_date'):
            in_data['sequencing_org_received_date'] = in_data.get('received_date')
        return in_data