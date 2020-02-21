package org.hj.hudi

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.junit.Test

class HiveViewTest {

  @Test
  def prestoViewRead(): Unit = {
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


  @Test
  def hiveViewRead(): Unit = {
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
