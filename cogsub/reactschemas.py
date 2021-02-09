from marshmallow import fields, Schema, validate, pre_load

class Samplemeta(Schema):

    central_sample_id = fields.Str()
    adm1 = fields.Str(missing='UK-ENG')
    collection_date = fields.Date()
    received_date = fields.Str(missing='2021-02-01')
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
        in_data['central_sample_id'] = in_data['cleaned_sample_name']
        in_data['biosample_source_id'] = in_data['sample_barcode']
        return in_data


class RunMeta(Schema):
    run_name = fields.Str()
    instrument_make = fields.Str(default='ILLUMINA')
    instrument_model = fields.Str(default='NextSeq 500')

class LibraryHeaderMeta(Schema):
    library_name = fields.Str()
    library_seq_kit = fields.Str(default='Nextera')
    library_seq_protocol = fields.Str(default='Nextera LITE')
    library_layout_config = fields.Str(default='PAIRED')

class LibraryBiosampleMeta(Schema):
    central_sample_id = fields.Str()
    library_selection = fields.Str(default='PCR')
    library_source = fields.Str(default='VIRAL_RNA')
    library_strategy = fields.Str(default='AMPLICON')
    library_protocol = fields.Str(default='ARTICv2')
    library_primers = fields.Integer(default=3)


    @pre_load
    def clean_up(self, in_data, **kwargs):
        in_data['central_sample_id'] = in_data['cleaned_sample_name']
        return in_data
