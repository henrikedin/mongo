call python buildscripts/clang_format.py format
call python ../../kernel-tools/codereview/upload.py -s mongodbcr.appspot.com --jira_user=henrik.edin %*