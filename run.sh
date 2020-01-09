#!/bin/sh
git add ./logv1tologv2 ./run.sh
git cifa
xargs < logging_cpp_files.txt ./logv1tologv2 
