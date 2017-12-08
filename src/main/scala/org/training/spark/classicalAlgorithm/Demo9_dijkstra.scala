package org.training.spark.classicalAlgorithm

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.training.spark.main.Singleton

object Demo9_dijkstra {
  def main(args: Array[String]): Unit = {
    val sc = Singleton.getSingleton
    sc.setLogLevel("WARN")
    val myVertices = sc.makeRDD(Array((1l, "a"), (2l, "b"), (3l, "c"), (4l, "d"), (5l, "e"), (6l, "f"), (7l, "g")))
    val myEdegs = sc.makeRDD(Array(Edge(1l, 2l, 7.0), Edge(1l, 4l, 5.0), Edge(2l, 3l, 8.0), Edge(2l, 4l, 9.0), Edge(2l, 5l, 7.0), Edge(3l, 5l, 5.0), Edge(4l, 5l, 15.0), Edge(4l, 6l, 6.0), Edge(5l, 6l, 8.0), Edge(5l, 7l, 9.0), Edge(6l, 7l, 11.0)
    ))

    val myGraph: Graph[String, Double] = Graph(myVertices, myEdegs)
    dijkstraTrace(myGraph, 1l).vertices.map(_._2).collect.foreach(println)
  }

  def dijkstra[VD](g: Graph[VD, Double], origin: VertexId): Graph[(VD, Double), Double] = {
    // 初始化
    var g2 = g.mapVertices {
      case (vid, _) =>
        val vd = if (vid == origin) 0 else Double.MaxValue
        (false, vd)
    }

    // 遍历所有的点
    (0l to g.vertices.count).foreach { i: Long =>
      val currentVertexId: VertexId = g2.vertices.filter(!_._2._1)
        .fold((0l, (false, Double.MaxValue))) {
          case (a, b) => if (a._2._2 < b._2._2) a else b
        }._1

      // 向当前顶点相邻的顶点发送消息,再聚合消息:取小值作为最短路径值
      val newDistances: VertexRDD[Double] = g2.aggregateMessages[Double](
        // sendMsg : 向邻边发送消息,内容为边的距离与做短路径直径之和
        ctx => if (ctx.srcId == currentVertexId) ctx.sendToDst(ctx.srcAttr._2 + ctx.attr),
        // megerMsg: 选择较小的值为当前定点的相邻顶点的最短路径值
        (a, b) => math.min(a, b)
      )

      g2 = g2.outerJoinVertices(newDistances) {
        (vid, vd, newSum) =>
          (vd._1 || vid == currentVertexId, math.min(vd._2, newSum.getOrElse(Double.MaxValue)))
      }
    }
    g.outerJoinVertices(g2.vertices) {
      (vid, vd, dist) =>
        (vd, dist.getOrElse((false, Double.MaxValue))._2)
    }
  }

  /**
    * 包含路径记录的Dijkstra最短路径算法
    * @param g
    * @param origin
    * @tparam VD
    * @return
    */
  def dijkstraTrace[VD](g: Graph[VD, Double], origin: VertexId): Graph[(VD, (Boolean, Double, List[VertexId])), Double] = {
    // 初始化，使用变量而不是常量是因为迭代计算算法要把每一轮迭代计算的结构赋值给一个变量以便在下一轮迭代中使用
    // 首先初始化变量G2,去掉原来的图G的顶点数据，添加由Boolean和Double组成的键值对。
    // Boolean变量表示顶点是否被访问过，Double表示源顶点到目标定点的距离，List[VertexId]记录当前进入的顶点Id
    var g2 = g.mapVertices {
      case (vid, vd) =>
        (false, if (vid == origin) 0 else Double.MaxValue, List[VertexId]())
    }
    // 遍历所有的点
    for (i <- 1l to g.vertices.count - 1) {
      // 确定最短路径值最小的作为当前的顶点
      val currentVertexId = g2.vertices.filter(!_._2._1)
        .fold((0l, (false, Double.MaxValue, List[VertexId]()))) {
          (a, b) => if (a._2._2 < b._2._2) a else b
        }._1
      // 向与当前顶点相邻的顶点发送消息,再聚合消息：取小值作为最短路径值
      val newDistances: VertexRDD[(Double, List[VertexId])] = g2.aggregateMessages[(Double, List[VertexId])](
        // sendMsg: 向邻边发消息,内容为边的距离与最短路径值之和
        ctx =>
          if (ctx.srcId == currentVertexId) ctx.sendToDst((ctx.srcAttr._2 + ctx.attr, ctx.srcAttr._3 :+ ctx.srcId)),
        // mergeMsg: 选择较小的值为当前顶点的相邻顶点的最短路径值
        (a, b) => if (a._1 < b._1) a else b
      )
      // 生成结果图
      g2 = g2.outerJoinVertices(newDistances)((vid,vd,newSum)=>{
        val newSumval = newSum.getOrElse((Double.MaxValue,List[VertexId]()))
        (vd._1 || vid == currentVertexId,math.min(vd._2,newSumval._1),if(vd._2 < newSumval._1) vd._3 else newSumval._2)
      })

    }
    // 最后一行返回值,用outerJoinVertices函数把新图G2的顶点属性重新保存到原来的图G中，
    // 这一步，return返回的图的顶点属性被更改，顶点属性类型是Tuple3[VD,Double,List[VertexId]],VD类型不变
    // Double类型对应的值被Dijkstra算法计算出的新距离值替换,List[VertexId]记录最短路径中经过的顶点ID
    g.outerJoinVertices(g2.vertices){
      case (vid,vd,dist)=>
        (vd,dist.getOrElse((false,Double.MaxValue,List[VertexId]())))
    }


  }
}
