#!/usr/bin/env python
import sys
#sys.path.append('./logparser')
from logparser.logparser import Drain3

input_dir  = 'logs/BGL/'  # The input directory of log file
output_dir = 'Drain_result/'  # The output directory of parsing results
log_file   = 'BGL.log'  # The input log file name
# 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.50.363779 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected
log_format = '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>'
# Regular expression list for optional preprocessing (default: [])
regex      = [r'core\.\d+']
st         = 0.3  # Similarity threshold
depth      = 3  # Depth of all leaf nodes

parser = Drain3.LogParser(log_format, indir=input_dir, outdir=output_dir,  depth=depth, st=st, rex=regex)
parser.parseFile(log_file, batch_size=10_000)

print(parser.getTemplateClusters())
