#!/usr/bin/env bash

cd /opt/project/pipeline/transform/stage_3/stage_3_transform

dbt run

if [ $? -ne 0 ]; then
  echo "___FAILED___: dbt run has failed."
  exit 1
fi

echo "___SUCCESS___: dbt run has succeeded."

dbt test

if [ $? -ne 0 ]; then
  echo "___FAILED___: dbt test has failed."
  exit 1
fi

echo "___SUCCESS___: dbt test has succeeded."