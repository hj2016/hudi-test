package org.hj.hudi

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.junit.Test

class HudiViewTest {


  /**
   * presto merge on read 实时视图查询
   */
  @Test
  def mergeOnReadRealtimeViewByPresto(): Unit = {
    // 目标表
    val sourceTable = "test_partition_merge_on_read_rt"
    Class.forName("com.facebook.presto.jdbc.PrestoDriver")
    val conn = DriverManager.getConnection("jdbc:presto://hj:7670/hive/hj_repl", "hive", null)
    val stmt = conn.createStatement
    val rs = stmt.executeQuery("select * from  " + sourceTable)
    val metaData = rs.getMetaData
    val count = metaData.getColumnCount

    while (rs.next()) {
      for (i <- 1 to count) {
        println(metaData.getColumnName(i) + ":" + rs.getObject(i).toString)
      }
      println("-----------------------------------------------------------")
    }

    rs.close()
    stmt.close()
    conn.close()
  }


  /**
   * presto merge on read 读优化视图查询
   */
  @Test
  def mergeOnReadReadoptimizedViewByPresto(): Unit = {
    // 目标表
    val sourceTable = "test_partition_merge_on_read_ro"
    Class.forName("com.facebook.presto.jdbc.PrestoDriver")
    val conn = DriverManager.getConnection("jdbc:presto://hj:7670/hive/hj_repl", "hive", null)
    val stmt = conn.createStatement
    val rs = stmt.executeQuery("select * from  " + sourceTable)
    val metaData = rs.getMetaData
    val count = metaData.getColumnCount

    while (rs.next()) {
      for (i <- 1 to count) {
        println(metaData.getColumnName(i) + ":" + rs.getObject(i).toString)
      }
      println("-----------------------------------------------------------")
    }

    rs.close()
    stmt.close()
    conn.close()
  }


  /**
   * merge on read 实时视图查询
   */
  @Test
  def mergeOnReadRealtimeViewByHive(): Unit = {
    // 目标表
    val sourceTable = "test_partition_merge_on_read_rt"

    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val prop = new Properties()
    prop.put("user", "hive")
    prop.put("password", "hive")
    val conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/hj_repl", prop)
    val stmt = conn.createStatement

    val rs = stmt.executeQuery("select * from " + sourceTable)
    val metaData = rs.getMetaData
    val count = metaData.getColumnCount


    while (rs.next()) {
      for (i <- 1 to count) {
        println(metaData.getColumnName(i) + ":" + rs.getObject(i).toString)
      }
      println("-----------------------------------------------------------")
    }

    rs.close()
    stmt.close()
    conn.close()
  }


  /**
   * merge on read 读优化视图查询
   */
  @Test
  def mergeOnReadReadoptimizedViewByHive(): Unit = {
    // 目标表
    val sourceTable = "test_partition_merge_on_read_ro"

    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val prop = new Properties()
    prop.put("user", "hive")
    prop.put("password", "hive")
    val conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/hj_repl", prop)
    val stmt = conn.createStatement

    val rs = stmt.executeQuery("select * from " + sourceTable)
    val metaData = rs.getMetaData
    val count = metaData.getColumnCount


    while (rs.next()) {
      for (i <- 1 to count) {
        println(metaData.getColumnName(i) + ":" + rs.getObject(i).toString)
      }
      println("-----------------------------------------------------------")
    }

    rs.close()
    stmt.close()
    conn.close()
  }

  /**
   * copy on write 读优化视图presto查询
   */
  @Test
  def copyOnWriteReadoptimizedViewByPresto(): Unit = {
    // 目标表
    val sourceTable = "test_partition"
    Class.forName("com.facebook.presto.jdbc.PrestoDriver")
    val conn = DriverManager.getConnection("jdbc:presto://hj:7670/hive/hj_repl", "hive", null)
    val stmt = conn.createStatement
    val rs = stmt.executeQuery("select * from  " + sourceTable)
    val metaData = rs.getMetaData
    val count = metaData.getColumnCount

    while (rs.next()) {
      for (i <- 1 to count) {
        println(metaData.getColumnName(i) + ":" + rs.getObject(i).toString)
      }
      println("-----------------------------------------------------------")
    }

    rs.close()
    stmt.close()
    conn.close()
  }

  /**
   * copy on write 读优化视图hive查询
   */
  @Test
  def copyOnWriteReadoptimizedViewByHive(): Unit = {
    // 目标表
    val sourceTable = "test_partition"

    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val prop = new Properties()
    prop.put("user", "hive")
    prop.put("password", "hive")
    val conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/hj_repl", prop)
    val stmt = conn.createStatement

    val rs = stmt.executeQuery("select * from " + sourceTable)
    val metaData = rs.getMetaData
    val count = metaData.getColumnCount


    while (rs.next()) {
      for (i <- 1 to count) {
        println(metaData.getColumnName(i) + ":" + rs.getObject(i).toString)
      }
      println("-----------------------------------------------------------")
    }

    rs.close()
    stmt.close()
    conn.close()

  }


  /**
   * copy on write 增量视图hive查询
   */
  @Test
  def copyOnWriteIncrementalViewByHive(): Unit = {
    // 目标表
    val sourceTable = "test_partition"
    // 增量视图开始时间点
    val fromCommitTime = "20200220094506"
    // 获取当前增量视图后几个提交批次
    val maxCommits = "2"

    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val prop = new Properties()
    prop.put("user", "hive")
    prop.put("password", "hive")
    val conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/hj_repl", prop)
    val stmt = conn.createStatement
    // 这里设置增量视图参数
    stmt.execute("set hive.input.format=org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat")
    // Allow queries without partition predicate
    stmt.execute("set hive.strict.checks.large.query=false")
    // Dont gather stats for the table created
    stmt.execute("set hive.stats.autogather=false")
    // Set the hoodie modie
    stmt.execute("set hoodie." + sourceTable + ".consume.mode=INCREMENTAL")
    // Set the from commit time
    stmt.execute("set hoodie." + sourceTable + ".consume.start.timestamp=" + fromCommitTime)
    // Set number of commits to pull
    stmt.execute("set hoodie." + sourceTable + ".consume.max.commits=" + maxCommits)

    val rs = stmt.executeQuery("select * from " + sourceTable)
    val metaData = rs.getMetaData
    val count = metaData.getColumnCount


    while (rs.next()) {
      for (i <- 1 to count) {
        println(metaData.getColumnName(i) + ":" + rs.getObject(i).toString)
      }
      println("-----------------------------------------------------------")
    }

    rs.close()
    stmt.close()
    conn.close()

  }


}
