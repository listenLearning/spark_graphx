package org.training.spark.graphX2Ml

import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.training.spark.main.Singleton

object Graphx2LogisticRegression {
  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val trainV = sc.makeRDD(Array((1L, (0, 1, false)), (2L, (0, 0, false)),
      (3L, (1, 0, false)), (4L, (0, 0, false)), (5L, (0, 0, false)),
      (6L, (0, 0, false)), (7L, (0, 0, false)), (8L, (0, 0, false)),
      (9L, (0, 1, false)), (10L, (0, 0, false)), (11L, (5, 2, true)),
      (12L, (0, 0, true)), (13L, (1, 0, false))))

    val trainE = sc.makeRDD(Array(Edge(1L, 9L, ""), Edge(2L, 3L, ""),
      Edge(3L, 10L, ""), Edge(4L, 9L, ""), Edge(4L, 10L, ""), Edge(5L, 6L, ""),
      Edge(5L, 11L, ""), Edge(5L, 12L, ""), Edge(6L, 11L, ""), Edge(6L, 12L, ""),
      Edge(7L, 8L, ""), Edge(7L, 11L, ""), Edge(7L, 12L, ""), Edge(7L, 13L, ""),
      Edge(8L, 11L, ""), Edge(8L, 12L, ""), Edge(8L, 13L, ""), Edge(9L, 2L, ""),
      Edge(9L, 13L, ""), Edge(10L, 13L, ""), Edge(12L, 9L, "")))

    val trainG = Graph(trainV, trainE)
    // 创建了1轮迭代的PageRank值和5轮迭代的PageRank值的join操作，并计算两个PageRank值的比例
    val jo: RDD[(VertexId, Double)] = PageRank.run(trainG, 1).vertices.join(
      PageRank.run(trainG, 5).vertices
    ).map(x => (x._1, x._2._2 / x._2._1))

    val trainSet: RDD[LabeledPoint] = augment(trainG, jo)
    // 训练逻辑回归模型，一般而言，SGD有时需要上百轮迭代才可以收敛，在此为了快速得到答案，仅迭代了10轮
    val model: LogisticRegressionModel = LogisticRegressionWithSGD.train(trainSet, 10)
    // 评估逻辑回归模型的效果
    val predict: Double = pref(trainSet, model)
    println(predict)
  }

  /**
    * 为了在LogisticRegressionWithSGD中使用图顶点数据，我们需要将它转化成算法所期待的输入格式，
    * 即存储着LabeledPoint格式数据的RDD，LabeledPoint格式是指特征向量再附加上人工打标的标签
    *
    * @param g
    * @param jo
    * @return
    */
  def augment(g: Graph[Tuple3[Int, Int, Boolean], String], jo: RDD[(VertexId, Double)]): RDD[LabeledPoint] = {
    g.vertices.join(jo)
      // LabeledPoint会使用向量来存储特征数据，在这种情况下，使用密集矩阵而不是稀疏矩阵，是因为我们的特征向量是不会有缺失值的，
      // 并且DenseVector更方便处理
      .map(x => LabeledPoint(if (x._2._1._3) 1 else 0, new DenseVector(Array(x._2._1._1, x._2._1._2, x._2._2))))
  }

  /**
    * 评估算法的效果，具体而言就是准确率指标，对比人工打标的标签以及model.predict产生的结果
    * @param trainSet
    * @param model
    * @return
    */
  def pref(trainSet: RDD[LabeledPoint], model: LogisticRegressionModel): Double = {
    // predict，接受特征向量作为输入并输出它认为最合适的标签
    // math.abs(model.predict(x.features) - x.label)：模型预测值和真实值的差别的绝对值.
    100 * (trainSet.count() - trainSet.map(x => math.abs(model.predict(x.features) - x.label)).reduce(_ + _)) / trainSet.count
  }
}
