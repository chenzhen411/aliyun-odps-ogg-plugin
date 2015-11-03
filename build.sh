#! /usr/bin/env bash

mvn clean package -DskipTests
cp -r target/datahub_lib build_1.0.0/
cd table-tool
mvn clean package -DskipTests
cp target/ogg-odps-table-1.0.0.jar ../build_1.0.0/table_tool/
cp -r target/dependency ../build_1.0.0/table_tool/
