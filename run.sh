#!/usr/bin/env sh

TEST_QUERIES="testcases"
INPUT="input"
OUTPUT="output"

QUERY_NO=$1

java QueryMain "$TEST_QUERIES/$INPUT/query$QUERY_NO.sql" "$TEST_QUERIES/$OUTPUT/out$QUERY_NO.txt" 200 3
