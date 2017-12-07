package org.training.spark.CommonAPI

import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.training.spark.CommonAPI.Demo2.sc
import org.training.spark.main.Singleton

object Demo6_ShortestPaths {
  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {

    val myVertices: RDD[(VertexId, String)] = sc.makeRDD(Array(
      (1l, "Ann"), (2l, "Bill"), (3l, "Charles"), (4l, "Diane"), (5l, "went to gym this morning")
    ))
    val myEdges: RDD[Edge[String]] = sc.makeRDD(Array(
      Edge(1l, 2l, "is-friends-with"),
      Edge(2l, 3l, "is-friends-with"),
      Edge(3l, 4l, "is-friends-with"),
      Edge(4l, 5l, "Likes-status"),
      Edge(3l, 5l, "Wrote-status")
    ))
    val myGraph: Graph[String, String] = Graph(myVertices, myEdges)
    ShortestPaths.run(graph = myGraph,Array(3l)).vertices.collect.foreach(println)
  }
}
