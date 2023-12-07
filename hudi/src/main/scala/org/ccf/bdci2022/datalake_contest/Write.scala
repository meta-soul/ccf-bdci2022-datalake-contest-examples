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
      .config("spark.kryoserializer.buffer.max", 1024)
      .config("hoodie.datasource.read.use.new.parquet.file.format", "true")

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin1")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

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
    val tablePath = "s3://ccf-datalake-contest/hudi/datalake_table"
    val tableName : String = "hudi"

    spark.time({
      val df = spark.read.format("parquet").load(dataPath0)

      df.write.format("hudi").mode("Overwrite")
        .option("hoodie.insert.shuffle.parallelism", "8")
        .option("hoodie.upsert.shuffle.parallelism", "8")
        .option(PRECOMBINE_FIELD.key(), "uuid")
        .option(RECORDKEY_FIELD.key(), "uuid")
        .option(TABLE_TYPE.key(), MOR_TABLE_TYPE_OPT_VAL)
        .option(OPERATION.key(), BULK_INSERT_OPERATION_OPT_VAL)
        .option("hoodie.bulkinsert.sort.mode", "NONE")
        .option("hoodie.index.type", "RECORD_INDEX")
        .option("hoodie.storage.layout.type", "BUCKET")
        .option("hoodie.index.bloom.num_entries", 10000000)
        .option("hoodie.bucket.index.num.buckets", 16)
        .option("hoodie.index.bucket.engine", "CONSISTENT_HASHING")
        .option("hoodie.bloom.index.use.metadata", true)
        .option("hoodie.parquet.max.file.size", "141557760")
        .option("hoodie.parquet.block.size", 32 * 1024 * 1024)
        .option("hoodie.parquet.compression.codec", "snappy")
        .option("hoodie.compaction.lazy.block.read", "true")
        .option("hoodie.parquet.small.file.limit", 104857600)
        .option("hoodie.memory.merge.fraction", 0.2)
        .option("hoodie.memory.compaction.fraction", 0.2)
        .option("hoodie.memory.merge.max.size", 5000000)
        .option("hoodie.memory.compaction.max.size", 5000000)
        .option("hoodie.commits.archival.batch", 2)
        .option("hoodie.compact.inline", false)
        .option("hoodie.compact.schedule.inline", false)
        .option("hoodie.write.buffer.limit.bytes", 1024 * 1024 * 10)
        .option("write.log_block.size", 8)
        .option("write.task.max.size", 64)
        .option("compaction.max_memory", 50)
        .option("write.parquet.block.size", 50)
        .option("write.merge.max_memory", 50)
        .option("hoodie.datasource.read.use.new.parquet.file.format", "true")
        .option("hoodie.spark.sql.insert.into.operation", "bulk_insert")
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
    })
  }

  private def upsertHudiTable(spark: SparkSession, tableName:String, tablePath: String, path: String): Unit = {
    println(s"upserting $path")
    val upsert_data = spark.read.format("parquet").load(path)
    upsert_data.write.format("hudi")
      .mode(SaveMode.Append)
      .option("hoodie.insert.shuffle.parallelism", "8")
      .option("hoodie.upsert.shuffle.parallelism", "8")
      .option(PRECOMBINE_FIELD.key(), "uuid")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .option(TBL_NAME.key(), tableName)
      .option("hoodie.parquet.max.file.size", "141557760")
      .option("hoodie.parquet.block.size", 32 * 1024 * 1024)
      .option("hoodie.index.type", "BUCKET")
      .option("hoodie.storage.layout.type", "BUCKET")
      .option("hoodie.index.bloom.num_entries", 10000000)
      .option("hoodie.bloom.index.use.metadata", true)
      .option("hoodie.bucket.index.num.buckets", 16)
      .option("hoodie.index.bucket.engine", "CONSISTENT_HASHING")
      .option("hoodie.parquet.compression.codec", "snappy")
      .option("hoodie.compaction.lazy.block.read", "true")
      .option("hoodie.parquet.small.file.limit", 104857600)
      .option("hoodie.memory.merge.fraction", 0.2)
      .option("hoodie.memory.compaction.fraction", 0.2)
      .option("hoodie.memory.merge.max.size", 50000000)
      .option("hoodie.memory.compaction.max.size", 50000000)
      .option("hoodie.commits.archival.batch", 2)
      .option("hoodie.compact.inline", true)
      .option("hoodie.compact.inline.max.delta.commits", 3)
      .option("hoodie.write.buffer.limit.bytes", 1024*1024*50)
      .option("write.log_block.size", 16)
      .option("write.task.max.size", 64)
      .option("compaction.max_memory", 50)
      .option("write.parquet.block.size", 50)
      .option("write.merge.max_memory", 50)
      .save(tablePath)
  }
}
