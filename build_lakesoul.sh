#!/usr/bin/env bash

set -ex

DIR="$(dirname "${BASH_SOURCE[0]}")"
DIR="$(realpath "${DIR}")"

cd "$DIR"

rm -rf lakesoul/target/jars && mkdir -p lakesoul/target/jars

mvn package -pl lakesoul -am -DskipTests

cp lakesoul/target/datalake_contest-1.0.0-SNAPSHOT.jar lakesoul/target/jars/datalake_contest.jar
cp lakesoul/lakesoul.properties lakesoul/target/jars

# mvn dependency:copy-dependencies -DoutputDirectory=target/jars -DincludeScope=runtime -DexcludeGroupIds=org.slf4j,org.apache.logging.log4j -pl lakesoul -DskipTests

tar czf lakesoul/target/datalake.tar.gz -C lakesoul/target/jars .
