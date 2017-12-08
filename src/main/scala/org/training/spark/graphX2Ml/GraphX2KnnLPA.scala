package org.training.spark.graphX2Ml

import java.awt.Color
import java.io.PrintWriter

import org.apache.commons.collections.map.HashedMap
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.training.spark.main.Singleton

import scala.collection.mutable
import scala.util.Random

case class knnVertex(classNum: Option[Int],
                     pos: Array[Double]) extends Serializable {
  /**
    * 计算点与点之间的权值(距离)
    *
    * @param that
    * @return
    */
  def dist(that: knnVertex): Double = math.sqrt(
    pos.zip(that.pos).map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _)
  )
}

/**
  * 算法过程：
  *   1.对于每天与有标顶点关联的边，将有标顶点的标签和相应的权重值(边距离的倒数)同时传给边的起始点和目标点
  *   2.对于每个顶点，分别(按标签划分)进行分数的累加。如果该顶点的标签不是预先确定的，则及进行标签修改，即将总分高的标签作为顶点的新标签
  *   3.如果没有顶点的标签发生变动，或者达到最大的迭代次数，结束
  */
object GraphX2KnnLPA {
  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")

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
    val g = knnGraphApprox(a, 4)
    val gs = semiSupervisedLabelPropagation(g = g)
    knnPredict(gs, Array(30.0, 30.0))
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
    *
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

  /**
    * 半监督学习标签传播算法
    * 本算法将图当作无向图来处理，实际的处理过程会有轻微不同，它会保证预先已确定标签的顶点永远不会改变自身的标签
    * 但总体而言标签可以沿着边的任意方向进行传播
    *
    * @param g
    * @param maxIterations
    * @return
    */
  def semiSupervisedLabelPropagation(g: Graph[knnVertex, Double], maxIterations: Int = 0) = {
    val maxIter: Long = if (maxIterations == 0) g.vertices.count / 2 else maxIterations

    var g2: Graph[(Boolean, knnVertex), Double] = g.mapVertices((_, vd) => (vd.classNum.isDefined, vd))
    var isChanged = true
    var i = 0

    do {
      val newV =
        g2.aggregateMessages[Tuple2[Option[Int], mutable.Map[Int, Double]]](
          ctx => {
            ctx.sendToSrc((ctx.srcAttr._2.classNum,
              if (ctx.dstAttr._2.classNum.isDefined)
                mutable.Map(ctx.dstAttr._2.classNum.get -> ctx.attr)
              else
                mutable.Map[Int, Double]()))
            if (ctx.srcAttr._2.classNum.isDefined)
              ctx.sendToDst((None,
                mutable.Map(ctx.srcAttr._2.classNum.get -> ctx.attr)))
          },
          (a1, a2) => {
            if (a1._1.isDefined)
              (a1._1, mutable.Map[Int, Double]())
            else if (a2._1.isDefined)
              (a2._1, mutable.Map[Int, Double]())
            else
              (None, a1._2 ++ a2._2.map {
                case (k, v) => k -> (v + a1._2.getOrElse(k, 0.0))
              })
          }
        )
      val newVClassVoted = newV.map(x => (
        x._1,
        if (x._2._1.isDefined)
          x._2._1
        else if (x._2._2.size > 0)
          Some(x._2._2.toArray.sortWith((a, b) => a._2 > b._2)(0)._1)
        else
          None
      ))
      isChanged = g2.vertices.join(newVClassVoted)
        .map(x => x._2._1._2.classNum != x._2._2)
        .reduce(_ || _)
      g2 = g2.joinVertices(newVClassVoted)((_, vd, u) =>
        (vd._1, knnVertex(u, vd._2.pos)))
      i += 1
    } while (i < maxIter && isChanged)
    g2.mapVertices((_, vd) => vd._2)
  }

  /**
    * 基于半监督学习所得的图的预测函数
    * 找到离pos最近的有标签的顶点(无论这个标签是预先确定的还是在传播过程中确定的)，然后返回该有标顶点的标签
    * 这是K=1时，K近邻预测方法(需要注意与K近邻图构建算法做区分)
    *
    * @param g
    * @param pos 坐标(x,y)点
    * @tparam E
    * @return
    */
  def knnPredict[E](g: Graph[knnVertex, E], pos: Array[Double]): Int = {
    g.vertices
      .filter(_._2.classNum.isDefined)
      .map(x => (x._2.classNum.get, x._2.dist(knnVertex(None, pos))))
      .min()(new Ordering[Tuple2[Int, Double]] {
        override def compare(x: (Int, Double), y: (Int, Double)) = {
          x._2.compare(y._2)
        }
      })
      ._1
  }
}
