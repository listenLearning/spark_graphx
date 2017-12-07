package org.training.spark.main

import org.apache.spark.graphx.{Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

object Demo2 {
  val sc: SparkContext = Singleton.getSingleton()
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val path = "Data/web-Google.txt"
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, path, canonicalOrientation = true, numEdgePartitions = 4)

    val tmp: Graph[Int, Int] = graph.mapVertices { case (_, attr) => attr * 3 }
    tmp.vertices.take(10).foreach(println)
    val tmp1 = graph.mapTriplets(e => e.srcAttr.toInt * 2 + e.dstAttr.toInt * 3)
    val tmp2 = graph.mapTriplets(e => e.attr.toInt * 5)
    val subgraph: Graph[Int, Int] = graph.subgraph(epred = e => e.srcId > e.dstId)
    print(s"vertexCount : ${graph.vertices.count()} , edgesCount : ${graph.edges.count()}")
    println(s"vertexCount : ${subgraph.vertices.count()} , edgesCount : ${subgraph.edges.count}")
    val subgraph1 = graph.subgraph(epred = e => e.srcId > e.dstId, vpred = (id, _) => id > 500000)
    subgraph1.triplets.take(10).foreach(println)
    val inDegrees = graph.inDegrees.filter { case (_, indegree) => indegree > 1000 }.sortBy(_._2, ascending = false)
    val outDegrees = graph.outDegrees.filter { case (_, indegree) => indegree > 1000 }.sortBy(_._2, ascending = false)
    val degrees = graph.degrees.filter { case (_, indegree) => indegree > 1000 }.sortBy(_._2, ascending = false)

    println(s"inDegree counts : ${inDegrees.count()} ,outDegrees count :${outDegrees.count()}, degrees counts : ${degrees.count}")
    println(s"max inDegree ${inDegrees.first()} ,max outDegree : ${outDegrees.first()} ,max degrees : ${degrees.first()}")
    println(s"max inDegree ${inDegrees.reduce(max)} ,max outDegree : ${outDegrees.reduce(max)} ,max degrees : ${degrees.reduce(max)}")

    val rawGraph: Graph[Int, Int] = graph.mapVertices((id, attr) => 0)
    rawGraph.vertices.take(10).foreach(println)
    val outDeg: VertexRDD[Int] = rawGraph.outDegrees
    val tmp3: Graph[Int, Int] = rawGraph.joinVertices[Int](outDeg)((_, _, optDeg) => optDeg)

    println

    val tmp4: Graph[Int, Int] = rawGraph.outerJoinVertices[Int, Int](outDeg)((_, _, optDeg) => optDeg.getOrElse(0))
    tmp4.vertices.take(10).foreach(println)
    val sourceID: VertexId = 0
    // Double.PositiveInfinity 正无穷大
    val g = graph.mapVertices((id, _) => if (id == sourceID) 0.0 else Double.PositiveInfinity)
    val sssp = g.pregel(Double.PositiveInfinity)(
      (_, dist, newDst) => math.min(dist, newDst),
      triple => {
        if (triple.srcAttr + triple.attr < triple.dstAttr) {
          Iterator((triple.dstId, triple.attr + triple.srcAttr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)
    )
    sssp.vertices.take(10).foreach(println)
    graph.pageRank(0.001).vertices.take(10).foreach(println)
    graph.triangleCount().vertices.take(10).foreach(println)

  }

  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }
}
