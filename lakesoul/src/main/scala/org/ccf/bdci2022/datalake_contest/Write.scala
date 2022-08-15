package org.ccf.bdci2022.datalake_contest

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ccf.bdci2022.datalake_contest.Write.overWriteTable

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
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://ccf-datalake-contest/datalake_table/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

    val spark = builder.getOrCreate()
    import spark.implicits._

    // tablePath: local test can use local path

    val dataPath0 = "s3://dmetasoul-bucket/lakesoul/CCF/base-0.parquet"
    val dataPath1 = "s3://dmetasoul-bucket/lakesoul/CCF/base-1.parquet"
    val dataPath2 = "s3://dmetasoul-bucket/lakesoul/CCF/base-2.parquet"
    val dataPath3 = "s3://dmetasoul-bucket/lakesoul/CCF/base-3.parquet"
    val dataPath4 = "s3://dmetasoul-bucket/lakesoul/CCF/base-4.parquet"
    val dataPath5 = "s3://dmetasoul-bucket/lakesoul/CCF/base-5.parquet"
    val dataPath6 = "s3://dmetasoul-bucket/lakesoul/CCF/base-6.parquet"
    val dataPath7 = "s3://dmetasoul-bucket/lakesoul/CCF/base-7.parquet"
    val dataPath8 = "s3://dmetasoul-bucket/lakesoul/CCF/base-8.parquet"
    val dataPath9 = "s3://dmetasoul-bucket/lakesoul/CCF/base-9.parquet"
    val dataPath10 = "s3://dmetasoul-bucket/lakesoul/CCF/base-10.parquet"

    val tablePath = "s3://dmetasoul-bucket/lakesoul/CCF/table_test"
    val df = spark.read.format("parquet").option("header", true).load(dataPath0).toDF()
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
  }

  def overWriteTable(spark: SparkSession, tablePath: String, path: String): Unit = {
    val df1 = spark.read.format("lakesoul").load(tablePath)
    val df2 = spark.read.format("parquet").option("header", true).load(path)
    df1.join(df2, Seq("uuid"),"left").select(
      df1("uuid"),
      when(df2("ip").isNotNull, df2("ip")).otherwise(df1("ip")).alias("ip"),
      when(df2("hostname").isNotNull, df2("hostname")).otherwise(df1("hostname")).alias("hostname"),
      when(df2("requests").isNotNull, df1("requests") + df2("requests")).otherwise(df1("requests")).alias("requests"),
      when(df2("name").isNotNull && df2("name").notEqual("null"), df2("name")).otherwise(df1("name")).alias("name"),
      when(df2("city").isNotNull, df2("city")).otherwise(df1("city")).alias("city"),
      when(df2("job").isNotNull, df2("job")).otherwise(df1("job")).alias("job"),
      when(df2("phonenum").isNotNull, df2("phonenum")).otherwise(df1("phonenum")).alias("phonenum")
    ).write.mode("Overwrite").format("lakesoul").save(tablePath)
  }

}
