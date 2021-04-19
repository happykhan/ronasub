import json 
from collections import Counter
from marshmallow import EXCLUDE
from cogschemas import Cogmeta, CtMeta

def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]

def most_frequent(List): 
    occurence_count = Counter(List) 
    return occurence_count.most_common(1)[0][0] 

def load_config(config="majora.json"):
    config_dict = json.load(open(config))
    return config_dict

def clean_dict(in_data, **kwargs):
    for k,v in dict(in_data).items():
        if v in ['', 'to check',  '#VALUE!', '-', None] :
            in_data.pop(k)        
        elif isinstance(v, str):
                in_data[k] = v.strip().upper()
    return in_data

def prepare_meta_record(x):
    # Should have library name and run name 
    if x.get('library_name') and x.get('run_name'):
        for k,v in dict(x).items():
            if v == '':
                x.pop(k)
        record = Cogmeta(unknown = EXCLUDE).load(x)
        up_record = Cogmeta().dump(record)
        ct_values = CtMeta(unknown=EXCLUDE).load(x)
        if up_record['central_sample_id'].startswith('NORW'):
            up_record["collection_pillar"] = 1
#        if up_record['central_sample_id'].startswith('ARCH'):
 #           up_record["collection_pillar"] = 4
        # Patient group should be biosample ID to pair 
        if len(up_record.get('patient_group', '')) > 4: 
            up_record['biosample_source_id'] = up_record['patient_group']         
        ct_1_info = clean_dict(dict(ct_value=ct_values.get('ct_1_ct_value'), test_kit=ct_values.get('ct_1_test_kit'), test_platform=ct_values.get('ct_1_test_platform'), test_target=ct_values.get('ct_1_test_target')))
        ct_2_info = clean_dict(dict(ct_value=ct_values.get('ct_2_ct_value'), test_kit=ct_values.get('ct_2_test_kit'), test_platform=ct_values.get('ct_2_test_platform'), test_target=ct_values.get('ct_2_test_target')))
        if ct_1_info or ct_2_info:
            up_record['metrics'] = dict(ct=dict(records={}))
            if ct_1_info:
                up_record['metrics']['ct']['records']['1'] = ct_1_info
            if ct_2_info:
                up_record['metrics']['ct']['records']['2'] = ct_2_info
        if len(x.get('epi_cluster', '')) > 3:
            up_record['metadata'] = dict(epi=dict(cluster=x.get('epi_cluster')))
        return up_record