package org.training.spark.commonAPI

import org.apache.spark.graphx.{Edge, Graph, GraphLoader, PartitionStrategy}
import org.training.spark.main.Singleton

object Demo5_TriangleRelation {
  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val path = "Data/Slashdot0811.txt"
    val graph = GraphLoader.edgeListFile(sc, path).cache
    val graph1 = Graph(graph.vertices, graph.edges.map(e => if (e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr))).partitionBy(PartitionStrategy.RandomVertexCut)
    val result: Seq[Int] = (0 to 6).map(i => graph1.subgraph(vpred = (vid, _) => vid >= i * 10000 && vid < (i + 1) * 10000)
      //triangleCount 实际上是基于每个顶点计算三角形关系,返回一个Graph[Int,ED]对象,然后合计所有顶点的三角形关系数(保存在定点数行中,通过_._2获取)
      .triangleCount.vertices.map(_._2).reduce(_ + _))
    println(result)
  }
}
