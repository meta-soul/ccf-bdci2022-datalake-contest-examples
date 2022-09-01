#!/usr/bin/env bash

set -ex

DIR="$(dirname "${BASH_SOURCE[0]}")"
DIR="$(realpath "${DIR}")"

cd "$DIR"

rm -rf target/jars && mkdir -p target/jars

mvn package -pl hudi -am -DskipTests

cp hudi/target/hudi-1.0.0-SNAPSHOT.jar target/jars/datalake_contest.jar

rm -f target/submit.zip
zip -r -j target/submit.zip target/jars/*
