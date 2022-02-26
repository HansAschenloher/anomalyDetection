#!/usr/bin/env python
import sys
sys.path.append('./logparser')
from logparser import Drain3

#log_file = sys.argv[1]

input_dir  = 'logs/Thunderbird/'  # The input directory of log file
output_dir = 'Drain_result/'  # The output directory of parsing results
log_file   = 'Thunderbird_10m.log'  # The input log file name
# - 1131524071 2005.11.09 tbird-admin1 Nov 10 00:14:31 local@tbird-admin1 postfix/postdrop[10896]: warning: unable to look up public/pickup: No such file or directory
log_format = '<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>'
# Regular expression list for optional preprocessing (default: [])
regex      = [r'(\d+\.){3}\d+']
st         = 0.3  # Similarity threshold
depth      = 3  # Depth of all leaf nodes

parser = Drain3.LogParser(log_format, indir=input_dir, outdir=output_dir,  depth=depth, st=st, rex=regex)
parser.parseFile(log_file, batch_size=100_000)


with open(output_dir+log_file+'_templates.csv', 'w') as f:
    parser.getTemplateClusters().to_csv(f)
