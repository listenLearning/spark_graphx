package org.training.spark.CommonAPI

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{GraphLoader, VertexId, VertexRDD}
import org.training.spark.main.Singleton

object Demo1_pageRank {
  val spark: SparkContext = Singleton.getSingleton
  spark.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val path = "Data/Cit-HepTh.txt"
    val graph = GraphLoader.edgeListFile(spark, path)
    //val graphNew: (VertexId, Int) = graph.inDegrees.reduce(max)
    /*println("max of outDegrees: " + graph.outDegrees.reduce(max) + " max of inDegrees: " + graph.inDegrees.reduce(max) + " max of Degrees: " + graph.degrees.reduce(max))
    println
    graph.vertices.take(10).foreach(println)
    println
    graph.edges.take(10).foreach(println)
    println
    // graph.pageRank方法返回的是(VertexId,Double)类型
    val v: VertexRDD[Double] = graph.pageRank(0.001).vertices
    println(v.reduce((a, b) => if (a._2 > b._2) a else b))*/
    // 个性化PageRank,在社交网络中判断"可能认识的人"时非常有用
    print(graph.personalizedPageRank(9207016,0.001)
      .vertices
      .filter(_._1 != 9207016)
      .reduce((a,b)=>if(a._2 > b._2) a else b))
  }

  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }
}
