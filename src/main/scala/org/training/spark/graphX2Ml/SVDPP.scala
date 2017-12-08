package org.training.spark.graphX2Ml

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.graphx.lib.SVDPlusPlus
import org.apache.spark.rdd.RDD
import org.training.spark.main.Singleton

object SVDPP {

  def main(args: Array[String]): Unit = {
    val sc = Singleton.getSingleton
    sc.setLogLevel("WARN")

    val edges: RDD[Edge[Double]] = sc.makeRDD(Array(
      Edge(1l, 11l, 5.0), Edge(1l, 12l, 4.0), Edge(2l, 12l, 5.0),
      Edge(2l, 13l, 5.0), Edge(3l, 11l, 5.0), Edge(3l, 13l, 2.0),
      Edge(4l, 11l, 4.0), Edge(4l, 12l, 4.0)
    ))
    /*算法的指定超参数解释：
    rank->2 隐形变量的个数
    maxIters->10 执行的迭代数。值越大，模型会更有可能收敛到理想的解决方案，预测率也会越高
    minVal->0 最低的评分
    maxVal->5 最高的评分
    gamma1->0.007 每次迭代中，偏差的改变速度，对应Koren论文里的γ1，推荐值为0.007
    gamma2->0.007 隐性变量的改变速度，对应koren论文里的γ2，推荐值为0.007
    gamma6->0.005 偏差的阻尼系数,用于保持偏差值不会过大。对应Koren论文中的λ6，所以该参数命名为lambda6可能更合适，推荐值为0.005
    gamma6-7->0.015 不同隐性变量被允许相互影响的程度，对应Koren论文中的λ7，所以该参数参数命名为lambda7可能更合适，推荐值为0.015*/
    // 在本例中，基于给定的影片有另种不同的流派类型，我们将秩设为2。这意味着，对于每个影片，算法的隐形变量的长度也相应为2，
    // 更大的数据集里可能会有更多的影片，这时秩的典型取值可以是10，20或者100
    val conf = new SVDPlusPlus.Conf(2, 10, 0, 5, 0.007, 0.007, 0.005, 0.015)
    // 运行SVD++算法，获得返回的模型——输入图的再处理结果和数据集的平均打分情况
    val (g, mean) = SVDPlusPlus.run(edges, conf)
    val result: Double = pred(g, mean, 4l, 3l)
    println(result)
  }

  /**
    * 注意：SVD++的一部分初始化时是使用随机数的，所以对于同一份输入数据，每次程序的预测结果不一定完全一样
    *
    * @param g    图的再处理结果
    * @param mean 数据集的平均打分情况
    * @param u    用户的VertexId
    * @param i    电影的VertexId
    * @return Double
    */
  def pred(g: Graph[(Array[Double], Array[Double], Double, Double), Double],
           mean: Double, u: Long, i: Long): Double = {
    //Graph[(Array[Double], Array[Double], Double, Double), Double]
    // Array[Double]：当为用户顶点的时候，对应论文中Pu；当为商品顶点时，为Qi
    // Array[Double]：当为商品顶点时，为Yi，当为用户顶点时，为Pu+|N(u)|^-1/2Sum_{j∈(u)}Yi
    // Double: 当为用户顶点时，为Bu；当为商品顶点时，为Bi
    // Double：当为用户顶点时，为|N(u)|^-1/2;当为商品顶点时，为|N(i)|^-1/2

    // 过滤得到顶点等于用户VertexId的值
    val user: (Array[Double], Array[Double], Double, Double) = g.vertices.filter(_._1 == u).collect()(0)._2
    // 过滤得到顶点等于电影VertexId的值
    val item = g.vertices.filter(_._1 == i).collect()(0)._2
    mean + user._3 + item._3 +
      // 使用scala时，可通过zip(),map()和reduce这几个函数来计算点积
      item._1.zip(user._2).map(x => x._1 * x._2).reduce(_ + _)
  }
}
