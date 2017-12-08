package org.training.spark.commonAPI

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.graphx.lib.LabelPropagation
import org.training.spark.main.Singleton

/**
  * 社区的定义：
  * 让稠密连接的顶点组在一个唯一的标签上达成一致，所以这些顶点组被定义为一个社区
  * 收敛定义：
  * 如果迭代算法在每轮迭代中都能保证接近一个特定结果，这事就可以说这个算法是"收敛"的
  * 标签传播算法LPA,实际上LPA不适合传播顶点分类的情况，即从已知分类的顶点向未知分类的顶点传播标签，因为它很难收敛
  */
object Demo8_LPA {
  def main(args: Array[String]): Unit = {
    val sc = Singleton.getSingleton
    sc.setLogLevel("WARN")

    val v = sc.makeRDD(Array((1l, ""), (2l, ""), (3l, ""), (4l, ""), (5l, ""), (6l, ""), (7l, ""), (8l, "")))
    val e = sc.makeRDD(Array(Edge(1l, 2l, ""), Edge(2l, 3l, ""), Edge(3l, 4l, ""), Edge(4l, 1l, ""), Edge(1l, 3l, ""), Edge(2l, 4l, ""), Edge(4l, 5l, ""), Edge(5l, 6l, ""), Edge(6l, 7l, ""), Edge(7l, 8l, ""), Edge(8l, 5l, ""), Edge(5l, 7l, ""), Edge(6l, 8l, "")))

    LabelPropagation.run(Graph(v, e), 5).vertices.collect.sortWith(_._1 < _._1).foreach(println)
    sc.stop()
  }
}
