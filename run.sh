#!/bin/sh
git add ./logv1tologv2 ./run.sh ./logging_cpp_files.txt
git cifa
xargs < logging_cpp_files.txt ./logv1tologv2 
