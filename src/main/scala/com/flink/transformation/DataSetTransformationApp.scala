package com.flink.transformation

import com.flink.dataset.Student
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object DataSetTransformationApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //mapFunction(env)
    //filterFunction(env)
    //mapPartitionFunction(env)
    //firstFunction(env)
    //flatMapFunction(env)
    //joinFunction(env)
    //reduceGroupFunction(env)
    //coGroupFunction(env)
    partitionHashFunction(env)
  }

  def filterFunction(env: ExecutionEnvironment): Unit = {
    env.fromCollection(1 to 10).filter(_ > 5).print()
  }

  def mapFunction(env: ExecutionEnvironment): Unit = {
    env.fromCollection(1 to 10).map(_ + 10).print()
  }

  def firstFunction(env: ExecutionEnvironment) = {
    val listBuffer = ListBuffer((1, "java"), (1, "scala"), (1, "C"), (2, "hadoop"), (2, "hdfs"), (3, "kafka"))
    env.fromCollection(listBuffer).groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print()
  }

  def flatMapFunction(env: ExecutionEnvironment) = {
    val data = env.readTextFile("./data/data.txt")
    data.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1).print()
  }

  def partitionHashFunction(env:ExecutionEnvironment)={
    val list = ListBuffer[(Int, String)]()
    list.append((1,"a"))
    list.append((2,"bb"))
    list.append((3,"ccc"))
    list.append((1,"b"))
    list.append((1,"c"))
    list.append((1,"d"))
    list.append((2,"aa"))
    list.append((2,"cc"))
    list.append((3,"aaa"))
    list.append((3,"bbb"))
    /*env.fromCollection(list).partitionByHash(0).mapPartition(item=>{
      println("current thread id :"+Thread.currentThread().getId)
      item
    }).print()*/
    env.fromCollection(list).partitionByRange(0).mapPartition(item=>{
      println("current thread id :"+Thread.currentThread().getId)
      item
    }).print()
  }

  /**
   * reduceGroup里面还要调用reduce方法
   * @param env
   */
  def reduceGroupFunction(env: ExecutionEnvironment)={
    val data = env.readTextFile("./data/data.txt")
    data.flatMap(_.split(" ")).map((_,1)).groupBy(0).reduceGroup((in:Iterator[(String,Int)],out:Collector[(String,Int)])=>{
      //val str = in.reduceLeft(_ + _)
      val str = in.reduceLeft((x,y)=>{
        (x._1,x._2+y._2)
      })
      out.collect(str)
    }).print()
  }

  def coGroupFunction(env: ExecutionEnvironment)={
    val info1 = ListBuffer[(Int,String)]()
    info1.append((1,"zzqq"))
    info1.append((2,"xx"))
    info1.append((3,"cc"))
    info1.append((4,"zz"))

    val info2 = ListBuffer[(Int,String)]((1,"jx"),(2,"sz"),(3,"hb"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)
    val value = data1.coGroup(data2).where(0).equalTo(0){
      (l,r)=>{
        if (r.isEmpty){
          (l.min,"_")
        }else{
          (l.min,r.max)
        }
      }
    }
    value.print()

    val value2 = data1.coGroup(data2).where(0).equalTo(0){
      (l,r,out: Collector[(Int,String)])=>{
        out.collect((l.min))
        out.collect((l.max))
      }
    }
    value2.print()
  }

  def joinFunction(env: ExecutionEnvironment)={
    val info1 = ListBuffer[(Int,String)]()
    info1.append((1,"zzqq"))
    info1.append((2,"xx"))
    info1.append((3,"cc"))
    info1.append((4,"zz"))

    val info2 = ListBuffer[(Int,String)]((1,"jx"),(2,"sz"),(3,"hb"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)
    data1.join(data2).where(0).equalTo(0).apply((left,right)=>{
      (left._1,left._2,right._2)
    }).groupBy(0).sortGroup(1,Order.ASCENDING).first(3).print()
  }

  /**
   * 根据并行度去获取连接
   *
   * @param env
   */
  def mapPartitionFunction(env: ExecutionEnvironment) = {
    val students = new ListBuffer[Student]
    for (i <- 1 to 100) {
      students.append(new Student("" + i, i, "" + i))
    }
    env.fromCollection(students).setParallelism(5).mapPartition(x => {
      val connection = DataSourceUtils.getConnection()
      DataSourceUtils.closeConnection(connection)
      x
    }).print()
  }
}
