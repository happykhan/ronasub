from marshmallow import Schema, fields, EXCLUDE, pre_load, validate
import datetime

class BioMeta(Schema):
    postcode_regex = '([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?)))))'
    get_counties = ['BEDFORDSHIRE', 'BERKSHIRE', 'BRISTOL', 'BUCKINGHAMSHIRE',
         'CAMBRIDGESHIRE', 'CHESHIRE', 'CITY OF LONDON', 'CORNWALL',
          'COUNTY DURHAM', 'CUMBRIA', 'DERBYSHIRE', 'DEVON', 'DORSET',
           'EAST RIDING OF YORKSHIRE', 'EAST SUSSEX', 'ESSEX', 'GLOUCESTERSHIRE', 'GREATER LONDON', 'GREATER MANCHESTER',
           'HAMPSHIRE', 'HEREFORDSHIRE', 'HERTFORDSHIRE', 'ISLE OF WIGHT', 'KENT', 'LANCASHIRE', 'LEICESTERSHIRE',
           'LINCOLNSHIRE', 'MERSEYSIDE', 'NORFOLK', 'NORTH YORKSHIRE', 'NORTHAMPTONSHIRE', 'NORTHUMBERLAND', 'NOTTINGHAMSHIRE',
           'OXFORDSHIRE', 'RUTLAND', 'SHROPSHIRE','SOMERSET','SOUTH YORKSHIRE','STAFFORDSHIRE','SUFFOLK','SURREY',
           'TYNE AND WEAR','WARWICKSHIRE','WEST MIDLANDS','WEST SUSSEX','WEST YORKSHIRE','WILTSHIRE','WORCESTERSHIRE', 'MIDDLESEX']

    central_sample_id = fields.Str(data_key="COG Sample ID", required=True, validate=validate.Regexp("^NORW-[a-zA-Z0-9]{5}$"))
    biosample_source_id = fields.Str(data_key="NNUH Sample ID", required=True)
    adm1 = fields.Str(missing="UK-ENG")
    adm2 = fields.Str(data_key="County", validate=validate.OneOf(get_counties))
    source_age = fields.Integer(data_key="Age", validate=validate.Range(min=0, max=120))
    source_sex = fields.Str(data_key="Sex", validate=validate.OneOf(['M','F']))
    received_date = fields.Str()
    collection_date = fields.Date(data_key="Collection date")
    sample_type_collected = fields.Str(data_key="Source", validate=validate.OneOf(["dry swab", "swab", "sputum", "aspirate"]))
    swab_site = fields.Str(data_key="Body site", validate=validate.OneOf(["nose", "throat", "nose-throat", "endotracheal", "rectal"]))
    collecting_org = fields.Str(data_key="Collecting organisation")
    library_name = fields.Str()
    library_seq_kit = fields.Str(missing='Nextera')
    library_seq_protocol = fields.Str(missing='Nextera LITE')
    library_layout_config = fields.Str(missing='PAIRED')
    library_selection = fields.Str(missing='PCR')
    library_source = fields.Str(missing='VIRAL_RNA')
    library_strategy = fields.Str(missing='AMPLICON')
    library_primers = fields.Integer(missing=3)
    library_protocol = fields.Str(missing='ARTICv2')
    run_name = fields.Str()
    previous_runs = fields.Str()
    instrument_make = fields.Str(missing='ILLUMINA')
    instrument_model = fields.Str(missing='NextSeq 500')
    adm2_private = fields.Str(data_key="Outer Postcode", validate=validate.Regexp(postcode_regex))
    date_sequenced = fields.Str()
    repeat_sample_id = fields.Str(data_key="Repeat Sample ID")
    is_icu_patient = fields.Str(data_key="ICU admission", validate=validate.OneOf(['Y','N', 'Unknown']))
   # ct_value = fields.Str(data_key='PCR Ct value')

    @pre_load
    def clean_up(self, in_data, **kwargs):
        if in_data.get('Collecting organisaton'):
            in_data['Collecting organisation'] = in_data.get('Collecting organisaton')
        for k,v in dict(in_data).items():
            if v in ['', 'to check'] :
                in_data.pop(k)        
            elif k in ['County', 'Collecting organisation', 'Outer Postcode'] and v.upper() in ['NOT AVAILABLE', 'UNKNOWN', 'NO ADDRESS', 'NO POST CODE']:
                in_data.pop(k)
            elif k in ['Sex'] and v.upper() in ['U', 'N']:
                in_data.pop(k)                
            elif k in ['ICU admission'] and v.upper() in ['U', 'UKNOWN']:       
                in_data.pop(k)         
            elif isinstance(v, str):
                    in_data[k] = v.strip()
        if in_data.get('Sex','').lower() in ['male']:
            in_data['Sex'] = 'M'                    
        if in_data.get('Sex','').lower() in ['female']:
            in_data['Sex'] = 'F'                                
        if in_data.get('Source','').lower() in ['bronchial washings','bronchial washing']:
            in_data['Source'] = 'aspirate'
        if in_data.get("County"):
            in_data["County"] = in_data["County"].upper()
        if in_data.get("County", '').upper() in ['CAMBS', 'CAMBRIDESHIRE', 'CAMBRIDGE', 'CAMBRIDGSHIRE']:
            in_data["County"] = 'CAMBRIDGESHIRE'
        if in_data.get("County", '').upper() == 'LINC':
            in_data["County"] = 'LINCOLNSHIRE'
        if in_data.get("County", '').upper() == 'LONDON':
            in_data["County"] = 'GREATER LONDON'            
        if in_data.get("County", '').upper() == 'COLCHESTER':
            in_data["County"] = 'ESSEX'            
        if in_data.get("Source"):
            in_data["Source"] = in_data["Source"].lower()            
        if in_data.get('Body site'):
            if in_data.get('Body site').lower() in ['throat/nose', 'nose/throat']:
                in_data['Body site'] = 'nose-throat'
            elif in_data.get('Body site').lower() in ['lung', "tracheostomy"]:
                in_data['Body site'] = 'endotracheal'
            elif in_data.get('Body site').lower() in ['mouth', 'throat/swab']:
                in_data['Body site'] = 'throat'                
            else:
                in_data['Body site'] = in_data.get('Body site').lower()
        if in_data.get('ICU admission', '').lower() in ['yes']:
            in_data['ICU admission'] = 'Y'
        if in_data.get('ICU admission', '').lower() in ['no']:
            in_data['ICU admission'] = 'N'   
        if  in_data.get('Collection date'):
            in_data['Collection date'] = self.handle_dates(in_data['Collection date'])            
        return in_data

    def handle_dates(self, date_string):
        try:
            datetime.datetime.strptime(date_string, '%Y-%m-%d')
            # String is fine, return itself. 
            return date_string
        except ValueError:
            try:
                datetime_obj = datetime.datetime.strptime(date_string, '%d/%m/%Y')
                return datetime_obj.strftime('%Y-%m-%d')
            except ValueError:
                try:
                    datetime_obj = datetime.datetime.strptime(date_string, '%d.%m.%Y')
                    return datetime_obj.strftime('%Y-%m-%d')
                except ValueError:
                    raise

        

class CtMeta(Schema):
    ct_1_ct_value = fields.Float()
    ct_1_test_kit = fields.Str()
    ct_1_test_platform = fields.Str()
    ct_1_test_target = fields.Str()
    ct_2_ct_value = fields.Float()
    ct_2_test_kit = fields.Str()
    ct_2_test_platform = fields.Str()
    ct_2_test_target = fields.Str()    


    @pre_load
    def clean_up(self, in_data, **kwargs):
        for k,v in dict(in_data).items():
            if v in ['', 'to check',  '#VALUE!', '-', 'N/A'] :
                in_data.pop(k)        
            elif isinstance(v, str):
                    in_data[k] = v.strip()
        return in_data

class lineageMeta(Schema):
    uk_lineage = fields.Str(data_key='peroba_uk_lineage')
    lineage = fields.Str(data_key='peroba_lineage')
    phylotype = fields.Str(data_key='peroba_phylotype')
    special_lineage = fields.Str(data_key='peroba_special_lineage')

    @pre_load
    def clean_up(self, in_data, **kwargs):
        for k,v in dict(in_data).items():
            if v in [''] :
                in_data.pop(k)        
            elif isinstance(v, str):
                    in_data[k] = v.strip()
        return in_data
