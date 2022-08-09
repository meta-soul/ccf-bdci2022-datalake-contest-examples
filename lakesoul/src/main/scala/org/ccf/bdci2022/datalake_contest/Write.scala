package org.ccf.bdci2022.datalake_contest

import com.dmetasoul.lakesoul.tables.LakeSoulTable

object Write {
  def main(args: Array[String]): Unit = {
    val table = LakeSoulTable.forPath("file:///tmp/test")
  }
}
