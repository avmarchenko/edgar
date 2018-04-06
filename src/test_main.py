# -*- coding: utf-8 -*-
"""
Basic Unittests
#####################
"""
from __future__ import absolute_import
import os
import unittest
import tempfile
from datetime import datetime
from collections import deque
from main import Task, App


class TestTask(unittest.TestCase):
    """Test unit operations of the Task class."""
    def setUp(self):
        self.task = Task("id0", 2, "%Y-%m-%d %H:%M:%S")
        header = "ip,date,time,zone,cik,accession,extention,code,size,idx,norefer,noagent,find,crawler,browser".split(",")
        line = "101.81.133.jja,2017-06-30,00:00:00,0.0,1608552.0,0001047469-17-004337,-index.htm,200.0,80251.0,1.0,0.0,0.0,9.0,0.0,".split(",")
        self.message = dict(zip(header, line))

    def test_clean(self):
        """Test :func:`Task.clean`."""
        self.assertDictEqual(self.task.clean(self.message),
                            {'accession': '0001047469-17-004337',
                             'browser': '',
                             'cik': '1608552.0',
                             'code': '200.0',
                             'crawler': '0.0',
                             'date': '2017-06-30',
                             'datetime': datetime(2017, 6, 30, 0, 0),
                             'extention': '-index.htm',
                             'find': '9.0',
                             'idx': '1.0',
                             'ip': '101.81.133.jja',
                             'noagent': '0.0',
                             'norefer': '0.0',
                             'size': '80251.0',
                             'time': '00:00:00',
                             'zone': '0.0'})

    def test_add(self):
        """Test :func:`Task.add`."""
        self.assertEqual(len(self.task), 0)
        self.task.add(self.message)
        self.assertEqual(len(self.task), 1)

    def test_flush(self):
        """Test :func:`Task.flush`."""
        self.task.messages = deque()
        self.task.add(self.message)
        self.assertEqual(len(self.task), 1)
        entry = self.task.flush(datetime.max)
        self.assertIsInstance(entry, deque)
        self.assertEqual(len(self.task), 0)


logfile = """ip,date,time,zone,cik,accession,extention,code,size,idx,norefer,noagent,find,crawler,browser
101.81.133.jja,2017-06-30,00:00:00,0.0,1608552.0,0001047469-17-004337,-index.htm,200.0,80251.0,1.0,0.0,0.0,9.0,0.0,
107.23.85.jfd,2017-06-30,00:00:00,0.0,1027281.0,0000898430-02-001167,-index.htm,200.0,2825.0,1.0,0.0,0.0,10.0,0.0,
107.23.85.jfd,2017-06-30,00:00:00,0.0,1136894.0,0000905148-07-003827,-index.htm,200.0,3021.0,1.0,0.0,0.0,10.0,0.0,
107.23.85.jfd,2017-06-30,00:00:01,0.0,841535.0,0000841535-98-000002,-index.html,200.0,2699.0,1.0,0.0,0.0,10.0,0.0,
108.91.91.hbc,2017-06-30,00:00:01,0.0,1295391.0,0001209784-17-000052,.txt,200.0,19884.0,0.0,0.0,0.0,10.0,0.0,
106.120.173.jie,2017-06-30,00:00:02,0.0,1470683.0,0001144204-14-046448,v385454_20fa.htm,301.0,663.0,0.0,0.0,0.0,10.0,0.0,
107.178.195.aag,2017-06-30,00:00:02,0.0,1068124.0,0000350001-15-000854,-xbrl.zip,404.0,784.0,0.0,0.0,0.0,10.0,1.0,
107.23.85.jfd,2017-06-30,00:00:03,0.0,842814.0,0000842814-98-000001,-index.html,200.0,2690.0,1.0,0.0,0.0,10.0,0.0,
107.178.195.aag,2017-06-30,00:00:04,0.0,1068124.0,0000350001-15-000731,-xbrl.zip,404.0,784.0,0.0,0.0,0.0,10.0,1.0,
108.91.91.hbc,2017-06-30,00:00:04,0.0,1618174.0,0001140361-17-026711,.txt,301.0,674.0,0.0,0.0,0.0,10.0,0.0,"""


output = """101.81.133.jja,2017-06-30 00:00:00,2017-06-30 00:00:00,1,1
108.91.91.hbc,2017-06-30 00:00:01,2017-06-30 00:00:01,1,1
107.23.85.jfd,2017-06-30 00:00:00,2017-06-30 00:00:03,4,4
106.120.173.jie,2017-06-30 00:00:02,2017-06-30 00:00:02,1,1
107.178.195.aag,2017-06-30 00:00:02,2017-06-30 00:00:04,3,2
108.91.91.hbc,2017-06-30 00:00:04,2017-06-30 00:00:04,1,1"""


class TestApp(unittest.TestCase):
    """Test full operation of the App class."""
    def setUp(self):
        """Set up app redirecting output to a pipe."""
        self.tempdir = tempfile.mkdtemp()
        self.log = os.path.join(self.tempdir(), "log.csv")
        with open(self.log, "w") as f:
            f.write(logfile)
        self.out = os.path.join(self.tempdir(), "out")
        self.app = App(self.log, 2, self.out)

    def tearDown(self):
        """Remove temporary files and directories."""
        os.remove(self.log)
        os.remove(self.out)
        os.rmdir(self.tempdir)

    def run(self):
        """Run app and check results."""
        self.app.run()
        self.assertEqual(len(self.tasks), 5)
        self.assertTrue(all(len(task.messages) == 0 for task in self.app.tasks.values()))
        self.assertListEqual(list(self.tasks.keys()), ['101.81.133.jja', '107.23.85.jfd',
                                                       '108.91.91.hbc', '106.120.173.jie',
                                                       '107.178.195.aag'])
        self.assertEqual(self.tasks.output_queue.qsize(), 0)
        with open(self.out) as f:
            check = f.read()
        self.assertEqual(output, check)
    

if __name__ == '__main__':
    unittest.main()
