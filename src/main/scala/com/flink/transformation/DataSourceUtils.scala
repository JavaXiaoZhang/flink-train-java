package com.flink.transformation


import scala.util.Random


object DataSourceUtils {
  def getConnection()= {
    val connection = Random.nextInt(10)
    println("get connection:"+connection)
    connection
  }

  def closeConnection(connection:Int): Unit ={
    println("close connection"+connection)
  }
}
