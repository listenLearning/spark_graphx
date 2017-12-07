package org.training.spark.main

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {
  val conf = new SparkConf().setMaster("local").setAppName("Spark_graph_demo1")
  val sc = SparkContext.getOrCreate(conf)
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val user: RDD[(VertexId, (String, String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    val relationShips: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")

    val graph: Graph[(String, String), String] = Graph(user, relationShips, defaultUser)

    graph.vertices.take(10).foreach{case (id,(name,occupation))=>println(s"userID :${id} ,name :${name} ,occupation :${occupation}")}
    println
    graph.edges.take(10).foreach{case Edge(src,dst,attr)=> println(s"原顶点: ${src}, 目标顶点: ${dst} ,关系: ${attr}")}
    println

  }


}
