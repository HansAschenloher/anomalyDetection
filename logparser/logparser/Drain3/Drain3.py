""" Description : This file implements the Drain algorithm for log parsing
Author      : Hans Aschenloher
License     : MIT
"""

import logging
import re
import os
from os.path import dirname
import sys
import time
import json
import numpy as np
import pandas as pd
import hashlib
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.file_persistence import FilePersistence
from datetime import datetime
from itertools import islice


logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')

class LogParser:
    def __init__(self, log_format, indir='./', outdir='./result/', depth=4, st=0.4,
                 maxChild=100, rex=[], keep_para=True, persistance_file=None):
        """
        Attributes
        ----------
            rex : regular expressions used in preprocessing (step1)
            path : the input path stores the input log file name
            depth : depth of all leaf nodes
            st : similarity threshold
            maxChild : max number of children of an internal node
            logName : the name of the input file containing raw log messages
            savePath : the output path stores the file containing structured logs
        """
        self.path = indir
        self.depth = depth
        self.st = st
        self.maxChild = maxChild
        self.logName = None
        self.savePath = outdir
        self.log_format = log_format
        self.rex = rex
        self.keep_para = keep_para


        persistence = None
        persistence_type = None
        if persistence != None:
            persistence_type = 'FILE'
            persistence = FilePersistence(persistance_file)

        config = TemplateMinerConfig()
        config.load(dirname(__file__) + "/drain3.ini")
        config.profiling_enabled = False
        config.drain_sim_th = self.st
        config.drain_depth = self.depth
        config.drain_max_children=self.maxChild

        self.template_miner = TemplateMiner(persistence, config)
        self.headers, self.regex = self.generate_logformat_regex(self.log_format)
        print(f"Drain3 started with '{persistence_type}' persistence")


    def parseLines(self, log_lines, lastLineId, callback=None):
        lineId = lastLineId

        logs_df = pd.DataFrame([],columns=self.headers)
        log_messages = []
        log_templates = []
        for line in log_lines:
            line = line.rstrip()
            try:
                lineId += 1
                match = self.regex.search(line)
                message = [match.group(header) for header in self.headers]
                log_messages.append(message)
            except Exception as e:
                pass
        logs_df = pd.DataFrame(log_messages, columns=self.headers)
        return self.parseDataframe(logs_df,lastLineId=lastLineId, callback=callback)

    def parseDataframe(self, logs_df, content_column = 'Content', callback = None, lastLineId=0):
        start_time = time.time()
        batch_start_time = start_time

        logs_df['EventTemplate'] = logs_df.get(content_column).map(lambda content: self.template_miner.add_log_message(str(content))['template_mined'])
        logs_df['EventId'] = logs_df['EventTemplate'].map(self.getTemplateId)
        logs_df['LineId'] = range(lastLineId+1,lastLineId+1+len(logs_df))

        time_took = time.time() - batch_start_time
        rate = len(logs_df) / time_took
        logger.info(f"Processing line: {lastLineId}, rate {rate:.1f} lines/sec, "
                      f"{len(self.template_miner.drain.clusters)} clusters so far.")
        batch_start_time = time.time()

        if callback:
            callback(logs_df)
        else:
            self.output(logs_df, lastLineId == 0)
        return lastLineId + len(logs_df)


    def getTemplateId(self, template):
        return hashlib.md5(template.encode('utf-8')).hexdigest()[0:8]

    def getTemplateClusters(self):
        clusters = self.template_miner.drain.clusters
        headers = ['EventId','EventTemplate', 'Occurrence']
        events = []
        for c in clusters:
            events.append([c.cluster_id, c.get_template(),c.size])
        return pd.DataFrame(events, columns=headers)


    def output(self, logdf, header=False):
        columns = ['LineId']
        columns.extend(list(filter(lambda x: x != 'LineId', logdf.columns)))
        logdf.to_csv(os.path.join(self.savePath, self.logName + '_structured.csv'), index=False, header=header, mode='a', columns=columns)


    def parseFile(self, log_file_name, batch_size=10000, callback=None, lastLineId=0):
        self.logName = log_file_name
        with open(os.path.join(self.path, log_file_name), 'r', encoding='ISO-8859-1') as file:
            for lines in iter(lambda: tuple(islice(file, batch_size)), ()):
                lastLineId = self.parseLines(lines, lastLineId, callback=callback)
        return lastLineId


    def generate_logformat_regex(self, logformat):
        """ Function to generate regular expression to split log messages
        """
        headers = []
        splitters = re.split(r'(<[^<>]+>)', logformat)
        regex = ''
        for k in range(len(splitters)):
            if k % 2 == 0:
                splitter = re.sub(' +', r'\\s+', splitters[k])
                regex += splitter
            else:
                header = splitters[k].strip('<').strip('>')
                regex += '(?P<%s>.*?)' % header
                headers.append(header)
        regex = re.compile('^' + regex + '$')
        return headers, regex
