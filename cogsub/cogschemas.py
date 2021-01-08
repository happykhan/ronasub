from marshmallow import fields, Schema, validate, pre_load

class Cogmeta(Schema):

    central_sample_id = fields.Str()
    adm1 = fields.Str()
    collection_date = fields.Date()
    received_date = fields.Date()
    source_age = fields.Integer(validate=validate.Range(min=0, max=110))
    source_sex = fields.Str(validate=validate.OneOf(["M", "F"]))
    adm2 = fields.Str()
    adm2_private = fields.Str()
    collecting_org = fields.Str()
    biosample_source_id = fields.Str()
    sample_type_collected = fields.Str()
    swab_site = fields.Str()
    is_surveillance = fields.Str(missing='Y')
    is_icu_patient = fields.Str(validate=validate.OneOf(["Y", "N"]))

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
                if in_data.get('collecting_org') == 'HMP NORWICH':
                    in_data['collecting_org'] = 'HMP'
        return in_data

class CtMeta(Schema):
    ct_1_ct_value = fields.Float(validate=validate.Range(min=0, max=2000))
    ct_1_test_kit = fields.Str(validate=validate.OneOf(["ALTONA", "ABBOTT", "INHOUSE", "ROCHE", "AUSDIAGNOSTICS", "BOSPHORE", "SEEGENE"]))
    ct_1_test_platform = fields.Str(validate=validate.OneOf(["APPLIED_BIO_7500","ALTOSTAR_AM16", "ABBOTT_M2000", "ROCHE_FLOW", "ROCHE_COBAS", "ELITE_INGENIUS", "CEPHEID_XPERT", "QIASTAT_DX", "AUSDIAGNOSTICS", "ROCHE_LIGHTCYCLER", "INHOUSE" ,"ALTONA", "PANTHER", "SEEGENE_NIMBUS"]))
    ct_1_test_target = fields.Str(validate=validate.OneOf(["S", "E", "RDRP", "N", "ORF1AB", "ORF8", "RDRP+N"]))    
    ct_2_ct_value = fields.Float(validate=validate.Range(min=0, max=2000))
    ct_2_test_kit = fields.Str(validate=validate.OneOf(["ALTONA", "ABBOTT", "INHOUSE", "ROCHE", "AUSDIAGNOSTICS", "BOSPHORE", "SEEGENE"]))
    ct_2_test_platform = fields.Str(validate=validate.OneOf(["APPLIED_BIO_7500","ALTOSTAR_AM16", "ABBOTT_M2000", "ROCHE_FLOW", "ROCHE_COBAS", "ELITE_INGENIUS", "CEPHEID_XPERT", "QIASTAT_DX", "AUSDIAGNOSTICS", "ROCHE_LIGHTCYCLER", "QIAGEN_ROTORGENE", "INHOUSE" ,"ALTONA", "PANTHER", "SEEGENE_NIMBUS"]))
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
    run_name = fields.Str()
    instrument_make = fields.Str()
    instrument_model = fields.Str()

class LibraryHeaderMeta(Schema):
    library_name = fields.Str()
    library_seq_kit = fields.Str()
    library_seq_protocol = fields.Str()
    library_layout_config = fields.Str()

class LibraryBiosampleMeta(Schema):
    central_sample_id = fields.Str()
    library_selection = fields.Str()
    library_source = fields.Str()
    library_strategy = fields.Str()
    library_protocol = fields.Str(default='ARTIC')
    library_primers = fields.Integer()