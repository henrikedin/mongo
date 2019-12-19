#!/bin/sh
/opt/mongodbtoolchain/v3/bin/python3 ./buildscripts/scons.py  \
    --ssl \
    -j16 \
    --variables-files=etc/scons/mongodbtoolchain_v3_gcc.vars \
    CPPDEFINES=MONGO_CONFIG_LOGV2_DEFAULT \
    -k \
    mongod mongo mongos
