package org.ccf.bdci2022.datalake_contest

import org.apache.spark.sql.SparkSession

object Write {

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("CCF BDCI 2022 DataLake Contest")
      .master("local[4]")
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("hadoop.fs.s3a.committer.name", "directory")
      .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
      .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/s3a")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3.buffer.dir", "/opt/spark/work-dir/s3")
      .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
      .config("spark.hadoop.fs.s3a.fast.upload", value = true)
      .config("spark.hadoop.fs.s3a.multipart.size", 67108864)
      .config("spark.hadoop.fs.s3a.connection.maximum", 1000)
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 8)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://ccf-datalake-contest/paimon/")
      .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .config("spark.sql.catalog.paimon.warehouse", "s3://ccf-datalake-contest/paimon/")

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin1")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql(
      """(?x)
        |CREATE TABLE paimon.default.datalake_table (
        |   uuid string,
        |   ip string,
        |   hostname string,
        |   requests int,
        |   name string,
        |   city string,
        |   job string,
        |   phonenum string)
        | USING paimon
        | TBLPROPERTIES (
        |   'primary-key' = 'uuid',
        |   'bucket'='4',
        |   'file.format' = 'parquet',
        |   'target-file-size' = '1g',
        |   # 'write-only'='true'
        |)
        |""".stripMargin)

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


    spark.time({
      mergeIntoTable(dataPath0, spark)
      mergeIntoTable(dataPath1, spark)
      mergeIntoTable(dataPath2, spark)
      mergeIntoTable(dataPath3, spark)
      mergeIntoTable(dataPath4, spark)
      mergeIntoTable(dataPath5, spark)
      mergeIntoTable(dataPath6, spark)
      mergeIntoTable(dataPath7, spark)
      mergeIntoTable(dataPath8, spark)
      mergeIntoTable(dataPath9, spark)
      mergeIntoTable(dataPath10, spark)
    })
  }

  private def mergeIntoTable(path: String, spark: SparkSession): Unit = {
    var insertTimes = 1
    if (path.contains("base-0")) {
      insertTimes = 1
    }
    for (_ <- 1 to insertTimes) {
      val df = if (insertTimes == 1) spark.read.format("parquet").load(path)
      else spark.read.format("parquet").load(path).sample(1.0 / insertTimes)
      df.createOrReplaceTempView("temp_view")
      spark.sql(
        """
          |MERGE INTO paimon.default.datalake_table t USING (SELECT * FROM temp_view) u ON t.uuid = u.uuid
          |WHEN MATCHED THEN
          |   UPDATE SET
          |     t.uuid = u.uuid,
          |     t.ip = u.ip,
          |     t.hostname = u.hostname,
          |     t.requests = u.requests,
          |     t.name = u.name,
          |     t.city = u.city,
          |     t.job = u.job,
          |     t.phonenum = u.phonenum
          |WHEN NOT MATCHED THEN INSERT *
          |""".stripMargin)
    }
  }
}
