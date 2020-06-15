import csv 
import os

all_passed = 0 
all_total = 0 
root = "/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq"
for egg in sorted(os.listdir(root)):
    if egg.startswith('result.illumina') and egg not in ['result.illumina.20200602', 'result.illumina.20200602.min_cov_500', 'result.illumina.20200602.no_corona', 'result.illumina.20200602.min_cov_1000', 'result.illumina.20200602.min_cov_100', ]:
        run_passed = 0
        run_total = 0 
        for qc in os.listdir(os.path.join(root, egg)):
            if qc.startswith('NORW'):
                file_path = os.path.join(root, egg, qc)
                
                for x in csv.DictReader(open(file_path)):
                    if x['qc_pass'] == 'TRUE':
                        run_passed += 1
                        all_passed += 1 
                    all_total += 1     
                    run_total += 1 
        print(f'{egg}: {run_passed} / {run_total} ')                    
print(f'TOTAL: {all_passed} / {all_total} ')