package org.hj.hudi

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test


class HudiTest {


  @Test
  def query(): Unit = {
    val basePath = "/tmp/hudi"
    val spark = SparkSession.builder.appName("query insert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val tripsSnapshotDF = spark.
      read.
      format("org.apache.hudi").
      load(basePath + "/*/*")

    tripsSnapshotDF.show()
  }

  @Test
  def insert(): Unit = {
    val spark = SparkSession.builder.appName("hudi insert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val insertData = spark.read.parquet("/tmp/1563959377698.parquet")
    insertData.write.format("org.apache.hudi")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      // 表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, "test")
      .mode(SaveMode.Overwrite)
      // 写入路径设置
      .save("/tmp/hudi")
  }


  @Test
  def upsert(): Unit = {

    val spark = SparkSession.builder.appName("hudi upsert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val insertData = spark.read.parquet("/tmp/1563959377699.parquet")

    insertData.write.format("org.apache.hudi")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, "test")
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      // 写入路径设置
      .save("/tmp/hudi");
  }


  @Test
  def delete(): Unit = {
    val spark = SparkSession.builder.appName("delta insert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val deleteData = spark.read.parquet("/tmp/1563959377698.parquet")
    deleteData.write.format("com.uber.hoodie")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, "test")
      // 硬删除配置
      .option(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY, "org.apache.hudi.EmptyHoodieRecordPayload")
  }


  @Test
  def insertPartition(): Unit = {
    val spark = SparkSession.builder.appName("hudi insert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    // 读取文本文件转换为df
    val insertData = Util.readFromTxtByLineToDf(spark, "/home/huangjing/soft/git/experiment/hudi-test/src/main/resources/test_insert_data.txt")
    insertData.write.format("org.apache.hudi")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 设置分区列
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "dt")
      // 设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // 设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(HoodieWriteConfig.TABLE_NAME, "test_partition")
      .mode(SaveMode.Overwrite)
      .save("/tmp/hudi")
  }


  @Test
  def queryPartition(): Unit = {
    val basePath = "/tmp/hudi"
    val spark = SparkSession.builder.appName("hudi query").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val tripsSnapshotDF = spark.
      read.
      format("org.apache.hudi").
      load(basePath + "/*/*")

    tripsSnapshotDF.show()
  }

  @Test
  def upsertPartition(): Unit = {

    val spark = SparkSession.builder.appName("upsert partition").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val upsertData = Util.readFromTxtByLineToDf(spark, "/home/huangjing/soft/git/experiment/hudi-test/src/main/resources/test_update_data.txt")

    upsertData.write.format("org.apache.hudi").option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 分区列设置
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "dt")
      .option(HoodieWriteConfig.TABLE_NAME, "test_partition")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save("/tmp/hudi");
  }

  @Test
  def upsertPartitionChange(): Unit = {

    val spark = SparkSession.builder.appName("delta upsert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val upsertData = Util.readFromTxtByLineToDf(spark, "/home/huangjing/soft/git/experiment/hudi-test/src/main/resources/test_partition_update_data.txt")

    upsertData.write.format("org.apache.hudi").option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "dt")
      .option(HoodieWriteConfig.TABLE_NAME, "test_partition")
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save("/tmp/hudi");
  }


  @Test
  def hiveSync(): Unit = {
    val spark = SparkSession.builder.appName("delta hiveSync").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val upsertData = Util.readFromTxtByLineToDf(spark, "/home/huangjing/soft/git/experiment/hudi-test/src/main/resources/hive_sync.txt")

    upsertData.write.format("org.apache.hudi")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 分区列设置
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "dt")
      // 设置要同步的hive库名
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "hj_repl")
      // 设置要同步的hive表名
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "test_partition")
      // 设置数据集注册并同步到hive
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      // 设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // 设置要同步的分区列名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt")
      // 设置jdbc 连接同步
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://localhost:10000")
      // hudi表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, "test_partition")
      // 用于将分区字段值提取到Hive分区列中的类,这里我选择使用当前分区的值同步
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      // 设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save("/tmp/hudi");
  }


}
