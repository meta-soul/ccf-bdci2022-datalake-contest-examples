package org.ccf.bdci2022.datalake_contest

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

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
      .config("spark.driver.memoryOverhead", "1500m")
      .config("spark.driver.memory", "14g")
      .config("spark.executor.memory", "14g")
      .config("spark.executor.memoryOverhead", "1500m")
      .config("spark.memory.fraction", "0.2")
      .config("spark.memory.storageFraction", "0.2")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.hadoop.fs.s3a.connection.maximum", 100)
      .config("hoodie.datasource.read.use.new.parquet.file.format", "true")
      .config("spark.kryoserializer.buffer.max", 1024)

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin1")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val tablePath = "s3://ccf-datalake-contest/hudi/datalake_table"

    spark.time({
      val df: DataFrame = spark.read.format("hudi").load(tablePath).select(col("uuid"), col("ip"), col("hostname"), col("requests"), col("name"),
        col("city"), col("job"), col("phonenum"))
        println(df.count())
    })
    spark.time({
      val df: DataFrame = spark.read.format("hudi").load(tablePath).select(col("uuid"), col("ip"), col("hostname"), col("requests"), col("name"),
        col("city"), col("job"), col("phonenum"))
      df.write.format("noop").mode("Overwrite").save()

    })
  }
}
