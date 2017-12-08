package org.training.spark.graphX2Ml

import java.awt.Color
import java.io.PrintWriter

import breeze.linalg.minMax
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.training.spark.main.Singleton

import scala.collection.mutable
import scala.util.Random


/**
  * 实现思路：
  *   1.实现一个K近邻图构建算法，作为无监督学习算法，并将他们应用于大部分是无标数据的数据集上
  *   2.实现一个简单的标签传播算法，将标签传递给附近的无标顶点上
  *   3.实现一个简单的knnPredict函数来预测新数据点的标签
  */
object graphX2KNN {
  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")
  case class knnVertex(classNum: Option[Int],
                       pos: Array[Double]) extends Serializable {
    /**
      * 计算点与点之间的权值(距离)
      * @param that
      * @return
      */
    def dist(that: knnVertex): Double = math.sqrt(
      pos.zip(that.pos).map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _)
    )
  }
  def main(args: Array[String]): Unit = {
    // 生成样本数据，Random.setSeed算法保证每次随机数都一致
    Random.setSeed(17l)
    val n = 10
    val a: Seq[knnVertex] = (1 to n * 2).map(i => {
      val x = Random.nextDouble()
      if (i <= n)
        knnVertex(if (i % n == 0) Some(0) else None, Array(x * 50,
          20 + (math.sin(x * math.Pi) + Random.nextDouble / 2) * 25))
      else
        knnVertex(if (i % n == 0) Some(1) else None, Array(x * 50 + 25,
          30 - (math.sin(x * math.Pi) + Random.nextDouble / 2) * 25))
    })
    // 在样本数据上运行K近邻算法并输出为.gexf文件
    val g1 = knnGraph(a, 4)
    val g2 = knnGraphApprox(a,4)
    val pw = new PrintWriter("knn.gexf")
    pw.write(toGexfWithViz(g1, 10))
    pw.close
  }

  /**
    * 暴力搜索K近邻
    *
    * @param a
    * @param k
    * @return
    */
  def knnGraph(a: Seq[knnVertex], k: Int): Graph[knnVertex, Double] = {
    val a2: Array[(Long, knnVertex)] = a.
      // a.zipWithIndex返回RDD[(knnVertex,Int)],第二个值是索引
      zipWithIndex.
      // swap操作，返回Seq[(Long,knnVertex)]
      map(x => (x._2.toLong, x._1))
      // 将上一步转换成Array，方便后续使用
      .toArray
    val v: RDD[(Long, knnVertex)] = sc.makeRDD(a2)
    val e: RDD[Edge[Double]] = v
      // 转换，将索引相同的knnVertex整合到一起，返回类似(0,(19,50.26881097213592)), (0,(18,55.47714742921841)))结构的数据
      .map(v1 => (v1._1, a2.map(v2 => (v2._1, v1._2.dist(v2._2)))
      // 排序，通过对比权值(距离)排序
      .sortWith((e, f) => e._2 < f._2)
      // 提取列表中从位置1到位置k + 1(不含该位置)的元素列表[1,k+1),在此处即为取出权值最前的几个列表
      .slice(1, k + 1)
      // 返回RDD[(Long,Array[Long])]形式的二元组，Array[Long]包含了离当前顶点最近的顶点
      .map(_._1)))
      // 计算点到点的权值?
      .flatMap(x => x._2.map(vid2 =>
        Edge(x._1, vid2,
          1 / (1 + a2(vid2.toInt)._2.dist(a2(x._1.toInt)._2)))))
    Graph(v, e)
  }

  /**
    * 将基于knnVertex的图定制化输出为(带布局的)Gephi的.gexf文件
    *
    * @param g
    * @param scale
    * @return
    */
  def toGexfWithViz(g: Graph[knnVertex, Double], scale: Double): String = {
    val colors = Array(Color.red, Color.green, Color.blue, Color.yellow, Color.pink, Color.magenta, Color.darkGray)
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" " +
      "xmlns:viz=\"http://www.gexf.net/1.1draft/viz\" " +
      "version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices.map(v =>
        "      <node id=\"" + v._1 + "\" label=\"" + v._1 + "\">\n" +
          "        <viz:position x=\"" + v._2.pos(0) * scale +
          "\" y=\"" + v._2.pos(1) * scale + "\" />\n" +
          (if (v._2.classNum.isDefined)
            "        <viz:color r=\"" + colors(v._2.classNum.get).getRed +
              "\" g=\"" + colors(v._2.classNum.get).getGreen +
              "\" b=\"" + colors(v._2.classNum.get).getBlue + "\" />\n"
          else "") +
          "      </node>\n").collect.mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges.map(e => "      <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"
  }

  /**
    * 分布式近似求解K近邻图生成
    * @param a
    * @param k
    * @return
    */
  def knnGraphApprox(a: Seq[knnVertex], k: Int): Graph[knnVertex, Double] = {
    val a2: Array[(Long, knnVertex)] = a.zipWithIndex.map(x => (x._2.toLong, x._1)).toArray
    val v: RDD[(Long, knnVertex)] = sc.makeRDD(a2)
    val n: Int = 3

    val minMax: (Double, Double, Double, Double) =
      v.map(x => (x._2.pos(0), x._2.pos(0), x._2.pos(1), x._2.pos(1)))
        .reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2),
          math.min(a._3, b._3), math.max(a._4, b._4)))
    val xRange: Double = minMax._2 - minMax._1
    val yRange: Double = minMax._4 - minMax._3
    val e: RDD[Edge[Double]] = calcEdges(0.0, v, minMax)(xRange, yRange, n, k, a2)
      .union(calcEdges(0.5, v, minMax)(xRange, yRange, n, k, a2))
      .distinct
      .map(x => (x.srcId, x))
      .groupByKey
      .map(x => x._2.toArray
        .sortWith((e, f) => e.attr > f.attr)
        .take(k))
      .flatMap(x => x)
    Graph(v, e)
  }

  /**
    *
    * @param offset
    * @param v
    * @param minMax
    * @param xRange
    * @param yRange
    * @param n
    * @param k
    * @param a2
    * @return Edge[Double]
    */
  def calcEdges(offset: Double, v: RDD[(Long, knnVertex)],
                minMax: (Double, Double, Double, Double))(
                 xRange: Double, yRange: Double, n: Int, k: Int,
                 a2: Array[(Long, knnVertex)]): RDD[Edge[Double]] =
    v.map(x => (math.floor((x._2.pos(0) - minMax._1) / xRange * (n - 1) + offset), x))
      // 为了让groupByKey按期望的方式进行分区和洗牌，需要设置它的可选参数指定分区数目，否则它会将一些较小的分区合并到一起
      .groupByKey(n * n)
      .mapPartitions(ap => {
        val af = ap.flatMap(_._2).toList
        af.map(v1 => (v1._1, af.map(v2 => (v2._1, v1._2.dist(v2._2)))
          .toArray
          .sortWith((e, f) => e._2 < f._2)
          .slice(1, k + 1)
          .map(_._1)))
          .flatMap(x => x._2.map(vid2 => Edge(x._1, vid2,
            1 / (1 + a2(vid2.toInt)._2.dist(a2(x._1.toInt)._2)))))
          .iterator
      })
}
