package org.training.spark.classicalAlgorithm

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.training.spark.main.Singleton

import scala.reflect.ClassTag

/**
  * kruskal算法无法遍历一次酒吧最终结果树构建出来，相反，
  * 它做一个全图搜索来发现具有最小全职的边，将它添加到边的集合中，
  * 最终构建出一棵树，算法描述如下：
  * 1.初始化集合中的边，构建一个空的最小生成树
  * 2.通过全图查找找到具有最小权值的边，符合以下两个条件就将其添加到结果集合中
  *   a.边不在结果集合中
  *   b.边不能与结果集合中的边构成环
  * 3.回到第2步，直到所有的顶点都包含在边的结果集中
  */
object Demo11_Krushal {
  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val myVertices = sc.makeRDD(Array((1l, "a"), (2l, "b"), (3l, "c"), (4l, "d"), (5l, "e"), (6l, "f"), (7l, "g")))
    val myEdegs = sc.makeRDD(Array(Edge(1l, 2l, 7.0), Edge(1l, 4l, 5.0), Edge(2l, 3l, 8.0), Edge(2l, 4l, 9.0), Edge(2l, 5l, 7.0), Edge(3l, 5l, 5.0), Edge(4l, 5l, 15.0), Edge(4l, 6l, 6.0), Edge(5l, 6l, 8.0), Edge(5l, 7l, 9.0), Edge(6l, 7l, 11.0)
    ))
    val myGraph: Graph[String, Double] = Graph(myVertices, myEdegs)

    val retMST = minSpanningTree(myGraph).triplets.map(et => (et.srcAttr, et.dstAttr)).collect()
    println(retMST.mkString(","))
  }

  def minSpanningTree[VD: ClassTag](g: Graph[VD, Double]): Graph[VD, Double] = {
    // 添加Boolean值到Edge边属性中,来表示这条边是否是生成树的最终边结果集合的一部分
    var g2 = g.mapEdges(e => (e.attr, false))
    for(i <- 1l to g.vertices.count() -1){
      val unavailabelEdge = g2.outerJoinVertices(
        // 在g2图中筛选出已经在结果集中存在的边，构造为一个子图，然后执行connectedComponents函数，
        // 通过计算每一个顶点连通分量关系，返回一个包含最小顶点ID的图，获取这个图的顶点集合，
        // 然后与使用outerJoinVertices图g2做连接
        g2.subgraph(_.attr._2)
        // connectedComponents连通组件算法一下子通过全图给出全部定点的连接信息。
        // 如果一条边的两个两个顶点属于相同接过书的连接分量，对kruskal算法来说这个边就不符合要求，
        // 因为添加到结果边集合中会生成环
        .connectedComponents().vertices
      )((_, vd, cid) => (vd, cid))
        .subgraph(et => et.srcAttr._2.getOrElse(-1) == et.dstAttr._2.getOrElse(-2))
        .edges
        .map(e => ((e.srcId, e.dstId), e.attr))
      type edgeType = ((VertexId, VertexId), Double)
      val smallestEdge =
        g2.edges
          .map(e => ((e.srcId, e.dstId), e.attr))
          .leftOuterJoin(unavailabelEdge)
          .filter(x => !x._2._1._2 && x._2._2.isEmpty)
          .map(x => (x._1, x._2._1._1))
          .min()(new Ordering[edgeType]() {
            override def compare(x: edgeType, y: edgeType) = {
              // 比较边的权重值
              val r = Ordering[Double].compare(x._2, y._2)
              // 如果边的权重值相等，则比较VertexId的大小，这是为了使执行具有确定性和可重复性
              if (r == 0)
                Ordering[VertexId].compare(x._1._1, y._1._1)
              else
                r
            }
          })
      g2 = g2.mapTriplets(et =>
        (et.attr._1, et.attr._2 || (et.srcId == smallestEdge._1._1
          && et.dstId == smallestEdge._1._2)
        )
      )
    }
    // minSpanningTree的返回值就是数本身，并不是全图，边属性类型被恢复成权值，临时的Boolean值被丢弃
    g2.subgraph(_.attr._2).mapEdges(_.attr._1)
  }

}
