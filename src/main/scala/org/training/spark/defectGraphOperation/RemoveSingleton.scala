package org.training.spark.defectGraphOperation

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.training.spark.main.Singleton

import scala.reflect.ClassTag

object RemoveSingleton {
  /**
    * 删除孤立顶点：在执行subgraph()之后使用
    * 实现思路:函数triplets返回的点集包含的都是有边的点
    *   1通过triplets获得所有起始点,调用triplets获得所有的目标点，通过union操作将它们合并起来
    *   2在顶点集上执行distinct操作后，就获得了一个用上述顶点集和原始边集构成的新图
    * @param g
    * @tparam VD
    * @tparam ED
    * @return
    */
  def removeSingletons[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]) = {
    Graph(g.triplets.map(et => (et.srcId, et.srcAttr))
      .union(g.triplets.map(et => (et.dstId, et.dstAttr)))
      .distinct, g.edges)
  }

  def main(args: Array[String]): Unit = {
    val sc = Singleton.getSingleton
    sc.setLogLevel("WARN")
    val vertices: RDD[(Long, String)] = sc.makeRDD(Seq(
      (1l, "Ann"), (2l, "Bill"), (3l, "Charles"), (4l, "Dianne")
    ))
    val edge: RDD[Edge[String]] = sc.makeRDD(Seq(
      Edge(1l, 2l, "is-friends-with"),
      Edge(1l, 3l, "is-friends-with"),
      Edge(4l, 1l, "has-blocked"),
      Edge(2l, 3l, "has-blocked"),
      Edge(3l, 4l, "has-blocked")
    ))

    val originalGraph: Graph[String, String] = Graph(vertices, edge)
    val subgraph = originalGraph.subgraph(et => et.attr.equalsIgnoreCase("is-friends-with"))

    //展示子图中的顶点，包括Dianne
    subgraph.vertices.foreach(println)
    println
    // 调用removeSingletons函数，并展示结果对应的顶点
    removeSingletons(subgraph).vertices.foreach(println)
  }
}
