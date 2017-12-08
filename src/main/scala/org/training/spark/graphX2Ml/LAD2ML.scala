package org.training.spark.graphX2Ml

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LDAModel}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.training.spark.main.Singleton

object LAD2ML {
  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val rddBags: RDD[List[(String, Int)]] = bagsFromDoucmentPerLine("Data/rcorpus")
    // 根据LAD所期待的输入形式，提供在全局词典里个单词的索引
    val vocab: Array[(String, Long)] = rddBags.
      // 将数据进行flatten(碾平)操作，返回RDD[(String,Int)]
      flatMap(x => x)
      // 根据key将相同的数据进行meger操作，在这里是将相同的key出现次数相加，有distinct的作用
      .reduceByKey(_ + _)
      // 获取key，此时的key进过reduceBykey操作，已经去确保是唯一的了
      .map(_._1)
      // 该函数返回ZippedWithIndexRDD，在ZippedWithIndexRDD中通过计算startIndices获得index；然后在compute函数中利用scala的zipWithIndex计算index
      .zipWithIndex().collect()
    // setK: 指明要将文档聚类到5个主题上，函数run返回值则是一个机器学习模型，
    // 里面提供了多种信息:每个主题的描述单词清单，每篇文档归属不同主题的程度
    val model = new LDA().setK(5).run(codeBags(rddBags, vocab))
    // 调用describeTopics查看每个主题的最相关的前6个单词
    model.describeTopics(6).map(_._1.map(vocab(_)._1)).foreach(println)
    // 查看第一篇文档的主题分布情况，由于调用topicDistributions会改变函数顺序，因此需要使用filter获取文档Id为0的文章
    model.asInstanceOf[DistributedLDAModel].topicDistributions.filter(_._1 == 0).collect.foreach(println)

  }

  def bagsFromDoucmentPerLine(filename: String): RDD[List[(String, Int)]] = {
    sc.textFile(filename).map(_.split(" ")
      // 将一些较短的单词(少于或等于5个字母)过滤掉，并且过滤掉特殊单词"reuter"
      .filter(x => x.length > 5 && x.toLowerCase != "reuter")
      // 将所有字母转为小写
      .map(_.toLowerCase)
      // 聚合操作，将相同的单词放在一起，返回Map[String,Array[String]]
      .groupBy(x => x)
      // 将Map转换成List，方便下一步计算单词出现次数,返回List[(String,Array[String])]
      .toList
      .map(x => (x._1, x._2.size))
    )
  }

  /**
    * codeBag函数前半部分是将词典里字符串形式的单词转换成对应的索引下标，
    * 后半部是是将它转换为LDA可处理的稀疏向量(SparseVector)形式-->
    * 首先将词袋转换成稀疏向量,然后使用zipWithIndex给每篇文档附上文档ID
    *
    * @param rddBags
    * @param vocab
    * @return
    */
  def codeBags(rddBags: RDD[List[Tuple2[String, Int]]], vocab: Array[Tuple2[String, Long]]): RDD[(Long, Vector)] = {
    rddBags.map(x =>
      // 将rddBags与vocab进行合并,并根据单词进行聚合
      (x ++ vocab).groupBy(_._1)
        // 将没有同时出现在两个清单中的元素过滤掉
        .filter(_._2.size > 1)
        .map(x => (x._2(1)._2.asInstanceOf[Long].toInt, x._2(0)._2.asInstanceOf[Int].toDouble)).toList
    ).zipWithIndex().map(x => (x._2, new SparseVector(
      vocab.size, x._1.map(_._1).toArray, x._1.map(_._2).toArray)
      .asInstanceOf[Vector]))
  }

}
