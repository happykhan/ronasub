import csv 
from os import path, mkdir, listdir


input_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20201216.old'
read_indir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20201216/ncovIllumina_sequenceAnalysis_trimPrimerSequences/'
seq_indir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20201216/ncovIllumina_sequenceAnalysis_makeConsensus/'
output_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20201216.swap'
read_outdir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20201216.swap/ncovIllumina_sequenceAnalysis_trimPrimerSequences/'
seq_outdir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20201216.swap/ncovIllumina_sequenceAnalysis_makeConsensus/'
if not path.exists(output_dir):
    mkdir(output_dir)
if not path.exists(read_outdir):
    mkdir(read_outdir)

map = {x['Wrong name']: x['Correct name'] for x in csv.DictReader(open("swaplist"), dialect=csv.excel_tab)} 
z = open('xxxxxx', 'w')
for x in listdir(read_indir):
    sample_name = x.split('_')[0]
    input_file = path.join(read_indir, x)
    z.write(f'cp {input_file}    {read_outdir}/{map.get(sample_name, sample_name)}.mapped.primertrimmed.sorted.bam\n'  )
for x in listdir(seq_indir):
    sample_name = x.split('_')[0]
    input_file = path.join(seq_indir, x)
    z.write(f'cp {input_file}    {read_outdir}/{map.get(sample_name, sample_name)}.mapped.primertrimmed.sorted.bam\n'  )    


