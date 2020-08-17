import logging

def get_surv_metadata(client, sheet_name='Sarcov_Sampling_info'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    surv = {}
    surv_counts = {}
    for x in all_values:
        surv_counts[str(x['run_date']) + '-' + x['region'].upper()] = dict(total_count = x['sample_count'], current_count=0)
        for y in x['org_list'].split(','):
            surv[y.strip()] = x['region'].upper()
    return surv, surv_counts

def update_surv_meta(surv, surv_counts, client, sheet_name='SARCOV2-Metadata'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    current_count = {}
    for x in all_values:
        this_org = x['collecting_org']
        this_region = surv.get(this_org)
        if this_region and x['run_name']:
            region_key = x['run_name'][0:6] + '-' +  this_region.upper()
            if surv_counts.get(region_key):
                if x.get('is_surveillance') == 'Y':
                    surv_counts[region_key]['current_count'] += 1 
    for y, x in surv_counts.items():
        if x['current_count'] != x['total_count']:
            logging.info(f'MISMATCH IN SAMPLING. There should be {x["total_count"]} YES for {y}. Actual value is {x["current_count"]}')
            valid_orgs = [k for k,v in surv.items() if y.split('-')[1] == v]
            logging.info(f'Valid orgs are ' + ','.join(valid_orgs))
    logging.info(surv_counts)
