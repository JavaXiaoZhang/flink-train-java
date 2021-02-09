package com.flink.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object DataSetDataSourceScalaApp {

  def fromCollection(env:ExecutionEnvironment): Unit ={
    val data = 1 to 10
    env.fromCollection(data).print()
  }

  def readTextFile(env: ExecutionEnvironment): Unit = {
    //env.readTextFile("./data/data.txt").print()
    env.readTextFile("./data").print()
  }

  def readCsvFile(env: ExecutionEnvironment): Unit = {
    // tuple方式指定泛型
    //env.readCsvFile[(String,Int,String)]("./data/data.csv",ignoreFirstLine=true).print()

    // case class方式指定泛型
    //env.readCsvFile[Student]("./data/data.csv",ignoreFirstLine=true,includedFields=Array(0,1,2)).print()

    // pojo方式指定泛型
    env.readCsvFile[com.flink.dataset.Student]("./data/data.csv",ignoreFirstLine=true,pojoFields = Array("name","age","gender")).print()
  }

  /**
   * 递归读取目录下的文件
   * @param env
   */
  def readRecursiveFiles(env: ExecutionEnvironment): Unit = {
    val conf = new Configuration
    conf.setBoolean("recursive.file.enumeration",true)
    env.readTextFile("./data").withParameters(conf).print()
  }

  def readCompressionFiles(env: ExecutionEnvironment): Unit = {
    env.readTextFile("./data/compression").print()
  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //fromCollection(env)
    //readTextFile(env)
    //readCsvFile(env)
    //readRecursiveFiles(env)
    readCompressionFiles(env)
  }

  case class Student (name:String,age:Int,gender:String)
}
