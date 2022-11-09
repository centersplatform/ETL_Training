package org.example

import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager
import java.sql.ResultSet

/**
 * @author ${user.name}
 */
object App {


  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    var con: Connection = null;
    var rs: ResultSet = null
    try {
      val conStr = "jdbc:hive2://10.111.86.81:9852/";
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      con = DriverManager.getConnection(conStr, "", "");
      val stmt = con.createStatement();
      //stmt.executeQuery("CREATE DATABASE emp")
      rs = stmt.executeQuery("show databases")
      while (rs.next()) { // Position the cursor                 3
        System.out.print(rs.getString(1))
        System.out.println()
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
    con.close()
  }

}