package org.ccf.bdci2022.datalake_contest

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.sql.{SaveMode, SparkSession}

object Write {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("CCF BDCI 2022 DataLake Contest")
      .master("local[4]")
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("hadoop.fs.s3a.committer.name", "directory")
      .config("spark.local.dir", "/opt/spark/work-dir/tmp")
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

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

    val spark = builder.getOrCreate()
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
    val tablePath = "s3://ccf-datalake-contest/datalake_table/hudi_test"
    val tableName : String = "hudi_test"
    val df = spark.read.format("parquet").load(dataPath0)

    df.write.format("hudi").mode("Overwrite")
      .option("hoodie.insert.shuffle.parallelism", "8")
      .option("hoodie.upsert.shuffle.parallelism", "8")
      .option(PRECOMBINE_FIELD.key(), "uuid")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PAYLOAD_CLASS_NAME.key(), "org.ccf.bcdi2022.datalake_contest.HudiCustomPayload")
      .option(TABLE_TYPE.key(), COW_TABLE_TYPE_OPT_VAL)
      .option(OPERATION.key(), BULK_INSERT_OPERATION_OPT_VAL)
      .option("hoodie.bulkinsert.sort.mode", "NONE")
      .option("hoodie.parquet.max.file.size", "141557760")
      .option("hoodie.parquet.block.size", "141557760")
      .option("hoodie.parquet.compression.codec", "snappy")
      .option(TBL_NAME.key(), tableName)
      .save(tablePath)

    upsertHudiTable(spark, tableName, tablePath, dataPath1)
    upsertHudiTable(spark, tableName, tablePath, dataPath2)
    upsertHudiTable(spark, tableName, tablePath, dataPath3)
    upsertHudiTable(spark, tableName, tablePath, dataPath4)
    upsertHudiTable(spark, tableName, tablePath, dataPath5)
    upsertHudiTable(spark, tableName, tablePath, dataPath6)
    upsertHudiTable(spark, tableName, tablePath, dataPath7)
    upsertHudiTable(spark, tableName, tablePath, dataPath8)
    upsertHudiTable(spark, tableName, tablePath, dataPath9)
    upsertHudiTable(spark, tableName, tablePath, dataPath10)

  }

  def upsertHudiTable(spark: SparkSession, tableName:String, tablePath: String, path: String): Unit = {
    val upsert_data = spark.read.format("parquet").load(path)
    upsert_data.write.format("hudi")
      .mode(SaveMode.Append)
      .option("hoodie.insert.shuffle.parallelism", "8")
      .option("hoodie.upsert.shuffle.parallelism", "8")
      .option(PRECOMBINE_FIELD.key(), "uuid")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PAYLOAD_CLASS_NAME.key(),"org.ccf.bcdi2022.datalake_contest.HudiCustomPayload")
      .option(OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .option(TBL_NAME.key(), tableName)
      .option("hoodie.parquet.max.file.size", "141557760")
      .option("hoodie.parquet.block.size", "141557760")
      .option("hoodie.parquet.compression.codec", "snappy")
      .save(tablePath)
  }

}
