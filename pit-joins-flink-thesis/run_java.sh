#!/usr/bin/env bash
export JAVA_PROGRAM_ARGS=`echo "$@"`

mvn clean package
mvn exec:java -Dexec.mainClass="test.Main" -Dexec.args="$JAVA_PROGRAM_ARGS"