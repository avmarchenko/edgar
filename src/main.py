#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
#########################################
EDGAR Analytics Insight Data Challenge
#########################################
"""
from __future__ import absolute_import
from __future__ import print_function
#from __future__ import division
import csv
import queue
import argparse    # Specific to this challenge, not a scalable solution
import threading
from collections import deque
from datetime import datetime


descr = """Insight edgar-analytics challenge. Template a scalable solution
for basic analysis of user sessions document access counts to the EDGAR system.

In order to be compatible with 'run_all_tests', use the default output filename,
'sessionization.txt'.
"""
parser = argparse.ArgumentParser(description=descr)
parser.add_argument("logfile", type=str, help="log.csv file path")
parser.add_argument("inactivity", type=str, help="Path to inactivity.txt")
parser.add_argument("-o", "--output", type=str, default="sessionization.txt",
                    help="Path to output file, with name sessionization.txt")
parser.add_argument("-d", "--delimiter", type=str, default=",", help="CSV delimiter")
parser.add_argument("-q", "--quotechar", type=str, default=None, help="CSV qualifier")


class Task(object):
    """Class that tracks and processes a user's EDGAR session."""
    def clean(self, message):
        """Clean a message."""
        message['datetime'] = datetime.strptime(" ".join((message['date'], message['time'])), self.dtfmt)
        message['init'] = datetime.now()
        return message
        
    def new(self, message):
        """Add a new message related to this task's id."""
        self.messages.append(self.clean(message))
        self.flush()

    def end(self):
        """Shutdown the task and flush all existing sessions."""
        pass

    def flush(self, init=False):
        """Perform introspection and flush any completed sessions."""
        popme = []
        now = datetime.now()
        for i, message in enumerate(self.messages):
            if (now - message['init']).total_seconds() >= self.period:
                popme.append(i)
        print(popme)
        if popme:
            datetimei = self.messages[popme[0]]['datetime']
            datetimef = self.messages[popme[-1]]['datetime']
            duration = (datetimef - datetimei).total_seconds()
            entry = self.entryfmt.format(ip=self.ip, count=len(popme), duration=duration,
                                         datetimef=datetimef.strftime(self.dtfmt),
                                         datetimei=datetimei.strftime(self.dtfmt))
            print(entry)
            self.messages = deque(message for i, message in enumerate(self.messages) if i not in popme)
        #threading.Timer(self.period, self.flush).start()
        
    def __len__(self):
        return len(self.messages)

    def __init__(self, ip, period, dtfmt="%Y-%m-%d %H:%M:%S"):
        self.ip = ip
        self.period = period
        self.messages = deque()
        self.dtfmt = dtfmt
        # entry format hardcoded because the output entry logic is hardcoded.
        self.entryfmt = "{ip},{datetimei},{datetimef},{duration},{count}"
        self.flush(True)
        
    def __repr__(self):
        return "Task(ip={}, messages={})".format(self.ip, len(self))


def output_writer():
    """
    Commit an entry to the output file.

    Args:
        message (dict): Keyword message from :class:`Task`
    """
    pass
    #while True:
    #    entry = output_queue.get()
    #    if entry is None:
    #        break
    #    write_output(entry)


#def _write_output(entry


def app(logfile, period, output, delimiter=",", quotechar=None, pkid="ip"):
    """
    Backend process that parses the log.csv file.

    Args:
        logfile (str): log.csv file path
        period (int): Inactivity period
        delimiter (str): CSV delimiter
        quotechar: Optional CSV qualifier
        pkid (str): Name of id field

    Note:
        This function mimics what a backend application or daemon might
        look-like, which receives requests and farms them out to workers.
    """
    tasks = {}    # Keys are ip addrs, values are instances of Task
    #output_write_thread = threading.Thread(target=output_writer)
    #output_write_thread.start()
    with open(logfile) as f:
        reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)
        # Expect the header to be the keys (in any order)
        header = next(reader)
        # and all subsequent lines to be request messages
        for line in reader:
            message = dict(zip(header,line))
            ip = message['ip']
            if ip not in tasks:
                tasks[ip] = Task(ip, period)
            tasks[ip].new(message)
    return tasks


def preprocessor():
    """
    This function processes the command line inputs containing the log.csv
    and inactivity_period.txt, which are used to simulate web requests.

    Note:
        This function exists due to the structure of inputs for this challenge.
    """
    args = parser.parse_args()
    with open(args.inactivity) as f:
        period = int(f.readline().strip())    # Value in seconds 1 to 86400
    app(args.logfile, period, args.output, args.delimiter, args.quotechar)


#if __name__ == "__main__":
#    output_queue = queue.Queue()
#    preprocessor()
