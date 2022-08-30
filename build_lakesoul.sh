#!/usr/bin/env bash

set -ex

DIR="$(dirname "${BASH_SOURCE[0]}")"
DIR="$(realpath "${DIR}")"

cd "$DIR"

rm -rf target/jars && mkdir -p target/jars

mvn package -pl lakesoul -am -DskipTests

cp lakesoul/target/lakesoul-1.0.0-SNAPSHOT.jar target/jars/datalake_contest.jar
cp lakesoul/lakesoul.properties target/jars

tar czf target/datalake.tar.gz -C target/jars .
