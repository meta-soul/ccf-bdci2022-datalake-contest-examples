package org.ccf.bdci2022.datalake_contest

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr}

import scala.collection.JavaConverters.dictionaryAsScalaMapConverter

object Read {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("CCF BDCI 2022 DataLake Contest")
      .master("local[4]")
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("hadoop.fs.s3a.committer.name", "directory")
      .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
      .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/s3a_staging")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3.buffer.dir", "/opt/spark/work-dir/s3")
      .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
      .config("spark.hadoop.fs.s3a.fast.upload", value = true)
      .config("spark.hadoop.fs.s3a.multipart.size", 67108864)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://ccf-datalake-contest/datalake_table/")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

    val spark = builder.getOrCreate()

//    val tablePath = "file:///Users/dudongfeng/work/zehy/hudi/table_test1"
    val tablePath = "s3://ccf-datalake-contest/datalake_table/hudi_test"

    val df: DataFrame = spark.read.format("hudi").load(tablePath).select(col("uuid"), col("ip"), col("hostname"), col("requests"), col("name"),
      col("city"), col("job"), col("phonenum"))
//    df.write.csv("/Users/dudongfeng/work/zehy/hudi/11")

    df.toDF.write.parquet("/opt/spark/work-dir/result/ccf/")


  }
}
