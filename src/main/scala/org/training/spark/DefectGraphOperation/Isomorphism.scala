package org.training.spark.DefectGraphOperation

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.graphx.lib.SVDPlusPlus
import org.apache.spark.rdd.RDD
import org.training.spark.main.Singleton

object Isomorphism {
  def func() = {

  }

  def main(args: Array[String]): Unit = {
    val sc = Singleton.getSingleton
    sc.setLogLevel("WARN")

    val path = "yago/yagoFacts.tsv"
    // 读取YAGO数据,创建一个二分图,并作为SVD++的输入来训练一个机器学习模型
    val gf = ReadRDF.readRdf(sc, path).subgraph(_.attr.equals("<exports>"))
    // 将exports的边对应的评分值设为1.0
    val e: RDD[Edge[Double]] = gf.edges.map(e => Edge(e.srcId, e.dstId, 1.0))
    // 在YAGO的<exports>边集上训练SVD++模型
    val conf = new SVDPlusPlus.Conf(2, 10, 0, 5, 0.007, 0.007, 0.005, 0.015)
    val (gs, mean) = SVDPlusPlus.run(e, conf)
    // 计算vr,缺失的加拿大潜在出口商品列表
    val gc: Graph[String, String] = removeSingleton.removeSingletons(gf.subgraph(et => et.srcAttr.equals("<Canada>")))
    val vr: RDD[VertexId] = e.map(x => (x.dstId, ""))
      .distinct
      .subtractByKey(gc.vertices)
      .map(_._1)

    // 查找没有记录在Wikipedia上的加拿大最有可能的出口商品
    val vm = vertexMap(gs)
    val cid: VertexId = gf.vertices.filter(_._2.equals("<Canada>")).first._1
    val r: RDD[(VertexId, Double)] = vr.map(v => (v, pred(vm, mean, cid, v)))
    val maxKey = r.max()(new Ordering[Tuple2[VertexId, Double]]() {
      override def compare(x: (VertexId, Double), y: (VertexId, Double)): Int = {
        Ordering[Double].compare(x._2, y._2)
      }
    })._1
    gf.vertices.filter(_._1 == maxKey).collect.foreach(println)

  }

  // map()--友好的SVD++预测函数和工具
  def pred(v: Map[VertexId, (Array[Double], Array[Double], Double, Double)],
           mean: Double, u: Long, i: Long) = {
    val user = v.getOrElse(u, (Array(0.0), Array(0.0), 0.0, 0.0))
    val item = v.getOrElse(i, (Array(0.0), Array(0.0), 0.0, 0.0))
    mean + user._3 + item._3 +
      item._1.zip(user._2).map(x => x._1 * x._2).reduce(_ + _)
  }

  def vertexMap(g: Graph[(Array[Double], Array[Double],
    Double, Double), Double]): Map[VertexId, (Array[Double], Array[Double], Double, Double)] = {
    g.vertices.collect.map(v => v._1 -> v._2).toMap
  }
}
