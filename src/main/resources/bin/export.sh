#!/usr/bin/env bash

JAVA_OPTS="$JAVA_OPTS"
JAVA_OPTS="$JAVA_OPTS -XX:-HeapDumpOnOutOfMemoryError -Xmx1G "
JAVA_OPTS="$JAVA_OPTS -cp \":`pwd`/etc/*:`pwd`/libs/*\""
JAVA_OPTS="$JAVA_OPTS -Dlogback.configurationFile=\"`pwd`/etc/logback.xml\""
JAVA_OPTS="$JAVA_OPTS -Dfile.encoding=UTF-8"

FULL_COMMAND="/usr/bin/java $JAVA_OPTS com.fnklabs.instic.Application export"

echo "Run command: $FULL_COMMAND"

eval "${FULL_COMMAND}"