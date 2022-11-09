package org.example
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.DriverManager
import java.sql.Connection

/**
 * A Scala JDBC connection example by Alvin Alexander,
 * https://alvinalexander.com
 */
object App {

  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val driver = "org.mariadb.jdbc.Driver"
    val url = "jdbc:mariadb://10.103.12.100:3306/hive3mr3"
    val username = "root"
    val password = "admin1234"

    // there's probably a better way to do this
    var connection:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("CREATE table emp(empno INT NOT NULL AUTO_INCREMENT ,ename varchar(20),PRIMARY KEY ( empno ))")

    } catch {
      case e: Exception => e.printStackTrace
    }
    connection.close()
  }

}