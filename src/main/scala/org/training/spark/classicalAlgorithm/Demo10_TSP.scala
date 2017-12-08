package org.training.spark.classicalAlgorithm

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.training.spark.main.Singleton

/**
  * 贪心算法步骤
  * 1.从某些顶点开始
  * 2.添加权重最小的邻边到生成树
  * 3.跳到第2步
  */
object Demo10_TSP {
  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val myVertices = sc.makeRDD(Array((1l, "a"), (2l, "b"), (3l, "c"), (4l, "d"), (5l, "e"), (6l, "f"), (7l, "g")))
    val myEdegs = sc.makeRDD(Array(Edge(1l, 2l, 7.0), Edge(1l, 4l, 5.0), Edge(2l, 3l, 8.0), Edge(2l, 4l, 9.0), Edge(2l, 5l, 7.0), Edge(3l, 5l, 5.0), Edge(4l, 5l, 15.0), Edge(4l, 6l, 6.0), Edge(5l, 6l, 8.0), Edge(5l, 7l, 9.0), Edge(6l, 7l, 11.0)
    ))

    val myGraph: Graph[String, Double] = Graph(myVertices, myEdegs)
    greedy(myGraph, 1l).vertices.collect.foreach(println)
  }

  /**
    * 贪心算法可以在不用增加太多代码的情况下,用不同的起始顶点重新运行整个算法，
    * 不断迭代，挑选出一个到达所有顶点并且最短的解决方案
    * @param g
    * @param origin
    * @tparam VD
    * @return Graph[Boolean, (Double, Boolean)]
    */
  def greedy[VD](g: Graph[VD, Double], origin: VertexId): Graph[Boolean, (Double, Boolean)] = {
    // 在循环中,与传入的参数对象G相比,对象G2有不同的顶点属性类型和边属性类型。
    // G2的顶点属性是Boolean类型，表示这个顶点是否已被最终方案采纳；边属性是Tuple2[Double,Boolean]类型，
    // Double是算法中用到的边权重值，Boolean表示这条边是否被采纳到最终方案里了
    var g2 = g.mapVertices((vid, vd) => vid == origin).mapTriplets {
      et => (et.attr, false)
    }

    var nextVertexId = origin
    var edgesAreAvailable = true
    // type是scala的保留字，用来对复杂的类型做别名，以防止在编译期一遍又一遍的声明很长的类型
    type tripletType = EdgeTriplet[Boolean, (Double, Boolean)]
    do {
      // availableEdges这个变量用于在来源和目标两个方向上检查边的有效性，也就是说，贪心算法用到的图是无向图
      // 但实际上所有的Graphx图都是有向图，有来源和目标两个方向，
      // 所以任何基于Graphx的无向图算法都要自己在来源和目标方向上做一些预先检查
      val availableEdges = g2.triplets
        .filter { et =>
          !et.attr._2 && (et.srcId == nextVertexId && !et.dstAttr
            || et.dstId == nextVertexId && !et.srcAttr)
        }
      edgesAreAvailable = availableEdges.count > 0
      if (edgesAreAvailable) {
        val smallestEdge = availableEdges.min()(new Ordering[tripletType]() {
          override def compare(x: tripletType, y: tripletType) = {
            Ordering[Double].compare(x.attr._1, y.attr._1)
          }
        })

        nextVertexId = Seq(smallestEdge.srcId, smallestEdge.dstId).filter(_ != nextVertexId).head
        // 由于Graphx中的图是不可变的，在算法迭代中，要对顶点和边的boolean属性重新设置一个新的boolean值，
        // 图有了改变，所以就要创建一个新的图赋值给G2变量，以便在下一轮迭代中继续使用
        g2 = g2.mapVertices((vid, vd) => vd || vid == nextVertexId)
          .mapTriplets { et =>
            (et.attr._1, et.attr._2 ||
              (et.srcId == smallestEdge.srcId && et.dstId == smallestEdge.dstId))
          }
      }
    } while (edgesAreAvailable)

    g2

  }
}
