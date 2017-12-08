package org.training.spark.commonAPI

import org.apache.spark.graphx.lib.{ConnectedComponents, StronglyConnectedComponents}
import org.apache.spark.graphx.{Edge, Graph}
import org.training.spark.main.Singleton

object Demo7_connectedComponents {
  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val graph: Graph[String, String] = Graph(sc.makeRDD((1l to 7l).map((_, ""))),
      sc.makeRDD(Array(Edge(2l, 5l, ""), Edge(5l, 3l, ""), Edge(3l, 2l, ""), Edge(4l, 5l, ""), Edge(6l, 7l, ""))).cache())
    // 在社交网络中，如果其他方面的算法被添加到推荐引擎中，增强联通组件是构成推荐引擎的基础
    // 另一个应用是确保在一个状态机中没有闭塞不通的死胡同，如果有则会被卡住
    // 当编译器做数据流分析来识别从来没有被用到的表达式时，增强连通组件在构建优化编译器时也很有用，否则会浪费计算资源
    StronglyConnectedComponents.run(graph,10).vertices.map((_.swap)).groupByKey().map(_._2).collect.foreach(println)
    println
    // 连通组件能在社交网络图中找到一些孤立的小圈子，并把他们在数据中心网络中区分开
    // connectedComponents函数返回一个与输入的图对象结构相同的新Graph对象
    // 连通组件是用用其中最小的顶点ID标示的，而这个最小的顶点ID会赋值给这个连通组件中的每个顶点属性
    graph.connectedComponents().vertices.map((_.swap)).groupByKey().map(_._2).collect.foreach(println)
    sc.stop()
  }
}
