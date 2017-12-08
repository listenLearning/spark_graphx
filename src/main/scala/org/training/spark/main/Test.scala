package org.training.spark.main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkException}

import scala.reflect.ClassTag

object Test {
  val conf = new SparkConf().setMaster("local").setAppName("testRDDMethod")
  val sc = new SparkContext(conf)
  Logger.getRootLogger.setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    //以Google的网页链接文件(后面由下载地址)为例，演示pregel方法，找出从v0网站出发，得到经过的步数最少的链接网站，类似于附近地图最短路径算法
    val graph: Graph[Double, Double] = GraphLoader.edgeListFile(sc, "Data/web-Google.txt", numEdgePartitions = 4).mapVertices((id, _) => id.toDouble).mapEdges(edge => edge.attr.toDouble)
    //定义源网页Id
    val sourceId: VertexId = 0

    val g = graph.mapVertices((id, attr) => if (id == 0) 0.0 else Double.PositiveInfinity)

    graph.vertices.take(10).foreach { v => println(s"verticsID : ${v._1}, vertexAttr :${v._2}") }
    println
    graph.edges.take(10).foreach { v => println(s"srcID : ${v.srcId}, dstID : ${v.dstId} , attr: ${v.attr}") }
    println
    graph.triplets.take(10).foreach { v => println(s"srcID: ${v.srcId} -> ${v.srcAttr}, dstID : ${v.dstId} -> ${v.dstAttr} , attr: ${v.attr}") }

    /*//pregel底层调用GraphOps的mapReduceTriplets方法，一会儿解释源代码
    val result = pregel[Double, Double, Double](g, Double.PositiveInfinity)(
      (id, vd, newVd) => math.min(vd, newVd),
      triplets => {
        if (triplets.srcAttr + triplets.attr < triplets.dstAttr) {
          Iterator((triplets.dstId, triplets.srcAttr + triplets.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)
    )
    //输出结果，注意pregel返回的是更新VertexId属性的graph，而不是VertexRDD[(VertexId,VD)]
    println("最短节点：" + result.vertices.filter(_._1 != 0).reduce(min))

    val map = Map("a" -> "1", "b" -> "2", "c" -> "3")
    println(show(map.get("a")) == "1")*/

  }

  //找出路径最短的点
  def min(a: (VertexId, Double), b: (VertexId, Double)): (VertexId, Double) = {
    if (a._2 < b._2) a else b
  }

  /**
    * 自定义收集VertexId的neighborIds
    *
    * @author TongXueQiang
    */
  def collectNeighborIds[T, U](edgeDirection: EdgeDirection, graph: Graph[T, U])(implicit m: scala.reflect.ClassTag[T], n: scala.reflect.ClassTag[U]): VertexRDD[Array[VertexId]] = {
    val nbrs =
      if (edgeDirection == EdgeDirection.Either) {
        graph.aggregateMessages[Array[VertexId]](
          ctx => {
            ctx.sendToSrc(Array(ctx.dstId)); ctx.sendToDst(Array(ctx.srcId))
          },
          _ ++ _, TripletFields.None)
      } else if (edgeDirection == EdgeDirection.Out) {
        graph.aggregateMessages[Array[VertexId]](
          ctx => ctx.sendToSrc(Array(ctx.dstId)),
          _ ++ _, TripletFields.None)
      } else if (edgeDirection == EdgeDirection.In) {
        graph.aggregateMessages[Array[VertexId]](
          ctx => ctx.sendToDst(Array(ctx.srcId)),
          _ ++ _, TripletFields.None)
      } else {
        throw new SparkException("It doesn't make sense to collect neighbor ids without a " +
          "direction. (EdgeDirection.Both is not supported; use EdgeDirection.Either instead.)")
      }
    graph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Array.empty[VertexId])
    }
  }

  /**
    * 自定义pregel函数
    *
    * @param graph           图
    * @param initialMsg      返回的vertexId属性
    * @param maxInterations  迭代次数
    * @param activeDirection 边的方向
    * @param vprog           更新节点属性的函数，以利于innerJoin操作
    * @param sendMsg         map函数，返回Iterator[A],一般A为Tuple2,其中id为接受消息方
    * @param mergeMsg        reduce函数，一般为合并，或者取最小、最大值……操作
    * @tparam A  想要得到的VertexId属性
    * @tparam VD graph中vertices的属性
    * @tparam ED graph中的edge属性
    * @return 返回更新后的graph
    */
  def pregel[A: ClassTag, VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], initialMsg: A, maxInterations: Int = Int.MaxValue, activeDirection: EdgeDirection = EdgeDirection.Either)(
    vprog: (VertexId, VD, A) => VD,
    sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A)
  : Graph[VD, ED] = {
    Prege10(graph, initialMsg, maxInterations, activeDirection)(vprog, sendMsg, mergeMsg) //调用apply方法
  }

  def innerJoin[U: ClassTag, VD: ClassTag](table: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, U) => VertexRDD[(VertexId, VD)]) = {
    val uf = (id: VertexId, data: VD, o: Option[U]) => {
      o match {
        case Some(u) => mapFunc(id, data, u)
        case None => data
      }
    }
  }

  def show(value: Option[String]): String = {
    value match {
      case Some(x) => x
      case None => "no value found!"
    }
  }
}

object Prege10 extends Logging {
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED], initialMsg: A,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A): Graph[VD, ED] = {
    //1.对vertices进行更新操作
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
    //2.compute the messages，注意调用的是mapReduceTriplets方法，源代码：
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    println("message:" + messages.take(10).mkString("\n"))

    var activeMessages = messages.count()
    //load
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {

      //3.Receive the messages.Vertices that didn't get any message do not appear in newVerts.
      //内联操作，返回的结果是VertexRDD，可以参看后面的调试信息
      val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
      print("newVerts:" + newVerts.take(10).mkString("\n"))

      //4.update the graph with the new vertices.
      //先把旧的graph备份，以利于后面的graph更新和unpersist掉旧的graph
      prevG = g

      //外联操作，返回整个更新的graph
      //getOrElse方法，意味，如果newOpt存在，返回newOpt,不存在返回old
      g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
      print(g.vertices.take(10).mkString("\n"))
      //新的graph cache起来，下一次迭代使用
      g.cache()

      //备份，同prevG = g操作一样
      val oldMessages = messages

      //Send new messages.Vertices that didn't get any message do not appear in newVerts.so
      //don't send messages.We must cache messages.so it can be materialized on the next line.
      //allowing us to uncache the previous iteration.
      //5下一次迭代要发送的新的messages,先cache起来
      messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).cache()

      print("下一次迭代要发送的messages:" + messages.take(10).mkString("\n"))

      //6
      activeMessages = messages.count()
      //如果activeMessages==0，迭代结束
      print("下一次迭代要发送的messages的个数：" + activeMessages)

      logInfo("Pregel finished iteration" + i);

      //原来，旧的message和graph不可用了，unpersist掉
      oldMessages.unpersist(blocking = false)
      newVerts.unpersist(blocking = false)
      prevG.unpersist(blocking = false)
      prevG.edges.unpersist(blocking = false)

      i += 1

    }
    g
  }
}