#!/bin/sh

hadoop fs -get /tmp/output.txt output.txt
./snapfunc -i:output.txt -o:foo
hadoop fs -put foo* /tmp/
