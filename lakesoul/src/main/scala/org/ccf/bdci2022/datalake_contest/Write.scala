package org.ccf.bdci2022.datalake_contest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf

object Write {
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
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 8)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://lakesoul-test-bucket/lakesoul/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin1")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1")

    val spark = builder.getOrCreate()
    SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, "true")
    spark.sparkContext.setLogLevel("ERROR")

    val dataPath0 = "/opt/spark/work-dir/data/base-0.parquet"
    val dataPath1 = "/opt/spark/work-dir/data/base-1.parquet"
    val dataPath2 = "/opt/spark/work-dir/data/base-2.parquet"
    val dataPath3 = "/opt/spark/work-dir/data/base-3.parquet"
    val dataPath4 = "/opt/spark/work-dir/data/base-4.parquet"
    val dataPath5 = "/opt/spark/work-dir/data/base-5.parquet"
    val dataPath6 = "/opt/spark/work-dir/data/base-6.parquet"
    val dataPath7 = "/opt/spark/work-dir/data/base-7.parquet"
    val dataPath8 = "/opt/spark/work-dir/data/base-8.parquet"
    val dataPath9 = "/opt/spark/work-dir/data/base-9.parquet"
    val dataPath10 = "/opt/spark/work-dir/data/base-10.parquet"

    val tablePath = "s3://lakesoul-test-bucket/lakesoul/datalake_table"

    spark.time({
      val df = spark.read.format("parquet").load(dataPath0)
      df.write.format("lakesoul").mode("Overwrite").save(tablePath)

      overWriteTable(spark, tablePath, dataPath1)
      overWriteTable(spark, tablePath, dataPath2)
      overWriteTable(spark, tablePath, dataPath3)
      overWriteTable(spark, tablePath, dataPath4)
      overWriteTable(spark, tablePath, dataPath5)
      overWriteTable(spark, tablePath, dataPath6)
      overWriteTable(spark, tablePath, dataPath7)
      overWriteTable(spark, tablePath, dataPath8)
      overWriteTable(spark, tablePath, dataPath9)
      overWriteTable(spark, tablePath, dataPath10)
    })
  }

  private def overWriteTable(spark: SparkSession, tablePath: String, path: String): Unit = {
    val df1 = spark.read.format("lakesoul").load(tablePath)
    val df2 = spark.read.format("parquet").load(path)
    df1.join(df2, Seq("uuid"),"full").select(
      col("uuid"),
      when(df2("ip").isNotNull, df2("ip")).otherwise(df1("ip")).alias("ip"),
      when(df2("hostname").isNotNull, df2("hostname")).otherwise(df1("hostname")).alias("hostname"),
      when(df2("requests").isNotNull, df2("requests")).otherwise(df1("requests")).alias("requests"),
      when(df2("name").isNotNull, df2("name")).otherwise(df1("name")).alias("name"),
      when(df2("city").isNotNull, df2("city")).otherwise(df1("city")).alias("city"),
      when(df2("job").isNotNull, df2("job")).otherwise(df1("job")).alias("job"),
      when(df2("phonenum").isNotNull, df2("phonenum")).otherwise(df1("phonenum")).alias("phonenum")
    ).write.mode("Overwrite").format("lakesoul").save(tablePath)
  }

}
