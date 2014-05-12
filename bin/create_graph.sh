#!/bin/sh

./snapfunc -g:e -n:2000 -d:5000 > /dev/null 2>&1
hadoop fs -put output.txt /tmp/output.txt
