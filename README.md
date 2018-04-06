# EDGAR Data Engineering Challenge

## Challenge Summary
This challenge simulates receiving streaming data, processing it
on the fly, and sending the result off to another system. The
challenge requires you to write a program that reads in a log
file, processes the data, and writes an output in the required
style/order. The focus is on building a working, scalable, well
documented, tested application.

## Information
This solution presents a solution using only Python standard libraries.
It was tested on Python 2.7 and Python 3.6. The structure used by
this solution is not the fastest for serial operation but can be
trivially distributed to a collection of workers.

## Usage
Command line usage can be obtained as follows:
    
    $ python ./src/main.py --help

Unit tests can be checked va:

    $ python ./src/test_main.py

The ``run.sh`` script is a wrapper for the test suite, but running
it directly can show how the code works.

    $ ./run.sh
    $ vimdiff output/*   # Use your favorite text comparison program

A number of tests are provided in this directory and can be run
using the ``run_tests.sh`` utility.

Please see [Insight](https://github.com/InsightDataScience/edgar-analytics)
for more information.
