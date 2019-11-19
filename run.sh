#!/bin/sh
git add ./logv1tologv2
git cifa
xargs < logging_cpp_files.txt ./logv1tologv2 
