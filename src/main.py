#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
#########################################
EDGAR Analytics Insight Data Challenge
#########################################
Challenge: construct an application that will parse log files from the
governments EDGAR system. The parsing function should generate a summary
log file with particular order based on parsed data. This challenge
simulates a backend app receiving streaming request information, analyzing
it on the fly, and passing off the summary results (as they appear) to
some other system, e.g. for analysis and visualization.

The goal of the implementation below is to showcase a solution that can
be trivially transformed into an app that has distributed workers in order
to be scaled up.
"""
from __future__ import absolute_import
from __future__ import print_function
import csv
try:    # py2 compatibility
    import queue
except ImportError:
    import Queue as queue
import argparse
from collections import deque, OrderedDict
from datetime import datetime


descr = """Insight edgar-analytics challenge. Template a scalable solution
for basic analysis of user sessions document access counts to the EDGAR system.

In order to be compatible with 'run_all_tests', use the default output filename,
'sessionization.txt'.
"""
parser = argparse.ArgumentParser(description=descr)
parser.add_argument("logfile", type=str, help="log.csv file path")
parser.add_argument("inactivity", type=str, help="path to inactivity_period.txt")
parser.add_argument("-o", "--output", type=str, default="sessionization.txt",
                    help="Path to output file, with name sessionization.txt")
parser.add_argument("-d", "--delimiter", type=str, default=",", help="CSV delimiter")
parser.add_argument("-q", "--quotechar", type=str, default=None, help="CSV qualifier")
parser.add_argument("-v", "--verbose", type=bool, default=False, help="verbose logging")
parser.add_argument("-k", "--pkid", type=str, default="ip", help="unique CSV key")
parser.add_argument("-e", "--entryfmt", type=str, help="log entry format",
                    default="{pkid},{datetimei},{datetimef},{duration},{count}")
parser.add_argument("-t", "--dtfmt", type=str, help="log date-time format",
                    default="%Y-%m-%d %H:%M:%S")


# This class becomes a, e.g., Celery task in a scalable solution
class Task(object):
    """Class that tracks and processes a user's EDGAR session."""
    def clean(self, message):
        """Clean message and add datetime."""
        message['datetime'] = datetime.strptime(" ".join((message['date'], message['time'])), self.dtfmt)
        return message
        
    def add(self, message):
        """Add a new message related to this task's id."""
        self.messages.append(self.clean(message))

    def flush(self, curdt):
        """
        Perform introspection and flush any completed sessions.
        
        Args:
            curdt (datetime): Current datetime encountered in parsing
        """
        if self.messages and (curdt - self.messages[-1]['datetime']).total_seconds() > self.period:
            entry = self.messages
            self.messages = deque()
            return entry
        
    def __len__(self):
        return len(self.messages)

    def __init__(self, pkid, period, dtfmt):
        self.pkid = pkid
        self.period = period
        self.dtfmt = dtfmt
        self.messages = deque()
        
    def __repr__(self):
        return "Task(pkid={}, messages={})".format(self.pkid, len(self))


class App(object):
    """
    Backend process that parses the log.csv file.

    Args:
        logfile (str): log.csv file path
        period (int): Inactivity period
        delimiter (str): CSV delimiter
        quotechar: Optional CSV qualifier
        pkid (str): Name of id field
        entryfmt (str): Format string of log file rows

    Note:
        This function mimics what a backend application or daemon might
        look-like, which receives requests and farms them out to workers.
    """
    _count_label = "_i_"    # Label used for internal counting
    
    def create_entry(self, messages, fmt=False):
        """
        Generates a text entry to be logged.
                
        Args:
            messages (iterable): Record array used to construct a log entry
            
        Returns:
            dct (dict): Dictionary of keyword arguments that can be formatted directly
        """
        datetimei = messages[0]['datetime']
        datetimef = messages[-1]['datetime']
        duration = int((datetimef - datetimei).total_seconds()) + 1
        return dict(pkid=messages[0][self.pkid], count=len(messages),
                    duration=duration,
                    datetimei=datetimei.strftime(self.dtfmt),
                    datetimef=datetimef.strftime(self.dtfmt))

    # In a scalable solution, may need to consider removing dormant Task instances
    def flush_tasks(self, t):
        """
        Update current tasks (sessions) time and flush completed sessions.
        
        Args:
            t (datetime): Current record's datetime
        """
        for task in self.tasks.values():
            entry = task.flush(t)
            if entry:
                self.output_queue.put(self.create_entry(entry))
                
    # This would be the target of a thread in a scalable solution
    def _output_writer(self):
        """
        Writes a single, correctly formated, entry to the output log.
        """
        with open(self.outpath, "w") as f:
            while not self.output_queue.empty():
                entry = self.output_queue.get()
                f.write(self.entryfmt.format(**entry)+"\n")
                self.output_queue.task_done()
                
    def cleanup(self):
        """
        Flush the final records, reordering as appropriate, and write the
        entries to the log.
        """
        entries = []
        for i, task in enumerate(self.tasks.values()):
            entry = task.flush(datetime.max)
            if entry:
                entry = self.create_entry(entry)
                entry[self._count_label] = i
                entries.append(entry)
        for entry in sorted(entries, key=lambda x: (x['datetimei'], x[self._count_label])):
            self.output_queue.put(entry)
        self._output_writer()
        self.output_queue.join()    # Wait until all entries have been written

    def run(self):
        """
        Process the log file and write the results to the output file.
        
        This function iterates over each line of the log file, creates a
        worker (:class:`~Task`) which handles all messages for a specific
        pkid (ip). Each task
        """
        with open(self.logfile) as f:
            reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quotechar)
            # Expect the header to be the keys (in any order)
            header = next(reader)
            # and all subsequent lines to be request messages
            for line in reader:
                message = dict(zip(header, line))    # Generate the request message
                pkid = message[self.pkid]
                if pkid not in self.tasks:
                    self.tasks[pkid] = Task(pkid, self.period, self.dtfmt)
                self.tasks[pkid].add(message)
                t = self.tasks[pkid].messages[-1]['datetime']
                self.flush_tasks(t)
        self.cleanup()
        if self.verbose:
            print("Finished!")

    def __init__(self, logfile, period, outpath, delimiter=",", quotechar=None, pkid="ip", verbose=False,
                 entryfmt="{pkid},{datetimei},{datetimef},{duration},{count}", dtfmt="%Y-%m-%d %H:%M:%S"):
        self.logfile = logfile
        self.period = period
        self.outpath = outpath
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.pkid = pkid
        self.dtfmt = dtfmt
        self.verbose = verbose
        self.entryfmt = entryfmt      # Not really useful right now since create_entry isn't flexible
        self.tasks = OrderedDict()    # Keys are ip addrs, values are instances of Task
        self.output_queue = queue.Queue()
        # For a scalable solution we would use a separate thread for processing the
        # queue on the fly instead of at the end; no logic needs to change regarding
        # the ordering of the final entries.

        
def launcher():
    """
    This function processes the command line inputs containing the log.csv
    and inactivity_period.txt, which are used to simulate web requests.

    Note:
        This function exists due to the structure of inputs for this challenge.
    """
    args = parser.parse_args()
    with open(args.inactivity) as f:
        period = int(f.readline().strip())    # Value in seconds 1 to 86400
    app = App(args.logfile, period, args.output, args.delimiter, args.quotechar,
              args.pkid, args.verbose, args.entryfmt, args.dtfmt)
    app.run()


if __name__ == "__main__":
    launcher()
