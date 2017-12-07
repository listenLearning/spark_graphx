package org.training.spark.CommonAPI

import org.apache.spark.graphx.{Edge, EdgeContext, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.training.spark.main.Singleton

object Demo2 {
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
    //myGraph.triplets.collect().foreach(println)
    //myGraph.inDegrees.collect().foreach(println)

    /*myGraph.mapTriplets(v => (v.attr, v.attr.equals("is-friends-with")
      && v.srcAttr.toLowerCase.contains("a")))
      .triplets
      .collect
      .foreach(println)*/
    /*myGraph.aggregateMessages[Int](
      ctx => {
        ctx.sendToDst(1)
        ctx.sendToSrc(1)
      }, _ + _
    ).rightOuterJoin(myGraph.vertices).map(x => (x._2._2, x._2._1.getOrElse(0))).collect().foreach(println)
    println
    myGraph.degrees.rightOuterJoin(myGraph.vertices).map(x => (x._2._2, x._2._1.getOrElse(0))).collect().foreach(println)*/
    /*val initialGraph = myGraph.mapVertices((_, _) => 0)
    propagateEdgeCount(initialGraph).vertices.collect().foreach(x => println(""))*/

    println(myGraph.degrees.reduce((a,b)=>if (a._2>b._2) a else b))

    sc.stop()
  }

  /**
    * sendMsg函数作为参数传入aggregateMessages
    * 这个函数会在图中的每条边上被调用.这里sendMsg只是简单的累加器
    *
    * @param ec
    */
  def sendMsg(ec: EdgeContext[Int, String, Int]): Unit = {
    ec.sendToDst(ec.srcAttr + 1)
  }

  /**
    * mergeMsg函数作为参数传入aggregateMessages,这个函数会在所有的消息传递到顶点后被重复调用
    * 消息经过合并后,最终得出结果为包含最大距离值的顶点
    *
    * @param a
    * @param b
    * @return
    */
  def mergeMsg(a: Int, b: Int): Int = {
    math.max(a, b)
  }

  def propagateEdgeCount(g: Graph[Int, String]): Graph[Int, String] = {
    // 生成新的顶点集
    val verts = g.aggregateMessages(sendMsg, mergeMsg)
    // 生成一个更新后的包含新的信息的图
    val g2 = Graph(verts, g.edges)
    /**
      * 将两组顶点连接在一起来看看更新后的图
      * 是否有任何新的信息——这会生成新的数据
      */
    val check = g2.vertices.join(g.vertices)
      //查看join顶点集合后的每个元素,并计算元素中的不同点,如果相同则返回0
      .map(x => x._2._1 - x._2._2)
      // 合计所有的不同,如果所有的顶点完全相同,合计结果为0
      .reduce(_ + _)
    if (check > 0) {
      // 如果有变化,则继续递归执行
      propagateEdgeCount(g2)
    } else {
      g
    }
  }
}
