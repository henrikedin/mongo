#!/bin/sh
git add ./logv1tologv2 ./run.sh ./logging_cpp_files.txt ./batcher.pl
git cifa
exec xargs ./logv1tologv2 
