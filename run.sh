#!/bin/sh
set -e
# save any work that we may be working on on these files
git add logv1tologv2 run.sh scons.sh logging_cpp_files.txt
# freshen our working directory
git checkout-index -f -a
# covert the files listed in logging_cpp_files
xargs < logging_cpp_files.txt ./logv1tologv2
( ./scons.sh 2> scons.out.txt ) || true
perl -n -e 'm/\[with T = (.*?);/ and print "$1\n"' scons.out.txt  | sort | uniq -c | sort -n > report.txt
cat report.txt
