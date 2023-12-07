#!/bin/bash

set -ex

BASEDIR=$(dirname "$0")
echo "Set pwd to ${BASEDIR}"
cd $BASEDIR

mkdir -p work-dir/data
rm -f work-dir/*.jar
unzip -q -o target/submit.zip -d work-dir/
# 如果数据不存在，则下载并解压数据
if [ `ls -1 work-dir/data/ 2>/dev/null | wc -l ` -gt 0 ];
then
    echo "data already exist"
else
    rm -f CCFDataTest.tar.gz
    echo "no data found, download it"
    wget https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/lakesoul/CCF/Test/CCFDataTest.tar.gz
    tar xf CCFDataTest.tar.gz -C work-dir/data
    rm -f CCFDataTest.tar.gz
fi

# 清理元数据
docker exec -ti ccf-bdci2022-local-env-lakesoul-meta-db-1 psql -h localhost -U lakesoul_test -d lakesoul_test -f /meta_cleanup.sql
# 清理 S3(Minio) 数据
#docker run --net ccf-bdci2022-local-env_default --rm -t swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2 aws --no-sign-request --endpoint-url http://minio:9000 s3 rm --recursive s3://ccf-datalake-contest/
# 清理结果数据
docker run --net ccf-bdci2022-local-env_default --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir bitnami/spark:3.3.1 rm -rf /opt/spark/work-dir/result
docker run --net ccf-bdci2022-local-env_default --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir bitnami/spark:3.3.1 rm -rf /opt/spark/work-dir/s3a

mkdir work-dir/result

start=`date +%s`
#start write
docker run --cpus 4 -m 25000m --net ccf-bdci2022-local-env_default -p 4040:4040 --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir  --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties  bitnami/spark:3.3.1 /bin/bash -c "spark-submit --driver-memory 14G --executor-memory 14G --conf spark.driver.memoryOverhead=1500m --conf spark.executor.memoryOverhead=1500m --driver-class-path \$(printf %s: /opt/spark/work-dir/*.jar) --class org.ccf.bdci2022.datalake_contest.Write --master local[4] /opt/spark/work-dir/datalake_contest.jar --localtest"
end=`date +%s`
wtime=`expr $end - $start`
echo $wtime

#check s3 write directory size
wres=`docker run --net ccf-bdci2022-local-env_default --rm -t swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2 aws  --no-sign-request --endpoint-url http://minio:9000 s3 ls --summarize --recursive s3://ccf-datalake-contest/datalake_table | grep "Total Size:" | awk '{if ($3>2000000000) print "true"; else print "false"}'`
#start read
start=`date +%s`
docker run --cpus 4 -m 25000m --net ccf-bdci2022-local-env_default -p 4040:4040 --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir  --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties  bitnami/spark:3.3.1 /bin/bash -c "spark-submit --driver-memory 14G --executor-memory 14G --conf spark.driver.memoryOverhead=1500m --conf spark.executor.memoryOverhead=1500m --driver-class-path \$(printf %s: /opt/spark/work-dir/*.jar) --class org.ccf.bdci2022.datalake_contest.Read --master local[4] /opt/spark/work-dir/datalake_contest.jar --localtest"
end=`date +%s`
rtime=`expr $end - $start`

# check read result and verify correctness with md5 checksum
set +e
docker run --cpus 4 -m 16000m --net ccf-bdci2022-local-env_default --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir -v ${PWD}/CCFCheck.py:/opt/spark/CCFCheck.py bitnami/spark:3.3.1  spark-submit --driver-memory 16G --executor-memory 16G --master "local[4]" /opt/spark/CCFCheck.py /opt/spark/work-dir/result/ccf merge 2d2b89ca48ad5594f9d35a8db6c7bdf72aa5105a187fcc9f5e81cd4aabd67d35
retVal=$?
if [ $retVal -eq 0 ]; then
  echo $wtime","$wres","$rtime",true" > CCFResult
else
  echo $wtime","$wres","$rtime",false" > CCFResult
fi

