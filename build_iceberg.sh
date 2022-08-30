#!/usr/bin/env bash

set -ex

DIR="$(dirname "${BASH_SOURCE[0]}")"
DIR="$(realpath "${DIR}")"

cd "$DIR"

rm -rf target/jars && mkdir -p target/jars

mvn package -pl iceberg -am -DskipTests

cp iceberg/target/iceberg-1.0.0-SNAPSHOT.jar target/jars/datalake_contest.jar
mvn dependency:copy-dependencies -DoutputDirectory=../target/jars -DincludeScope=runtime -pl iceberg -am

rm -f target/submit.zip
zip -r -j target/submit.zip target/jars/*
