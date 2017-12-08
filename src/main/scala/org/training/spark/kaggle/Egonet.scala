package org.training.spark.kaggle

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.training.spark.main.Singleton

object Egonet {
  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val path = "egonets/*"
    val rdd = sc.wholeTextFiles(path)
    val egonet_numbers: Array[String] = rdd.map(x => extract(x._1)).collect
    val egonet_edges: Array[Array[(Long, Long)]] = rdd.map(x => make_edges(x._2)).collect
    val egonet_circles: Seq[String] = egonet_edges.toList.map(x => get_circles(x))
    println("UserId,Prediction")
    val result = egonet_numbers.zip(egonet_circles).map(x => x._1 + "," + x._2)
    println(result.mkString("\n"))

  }

  /**
    * 从<path>/<userid>.egonet格式的文件路径中解析出用户ID
    *
    * @param s
    */
  def extract(s: String): String = {
    val pattern = "^.*?(\\d+).egonet"
    val p = Pattern.compile(pattern)
    val m = p.matcher(s)
    if (m.find) m.group(1) else "NO MATCH"
  }

  /**
    * 处理egonet文件的每行数据，返回元组形式的边数据
    *
    * @param line
    * @return Array[(Long, Long)]
    */
  def get_edges_from_line(line: String): Array[(Long, Long)] = {
    val ary = line.split(":")
    val srcId = ary(0).toLong
    val dstIds = ary(1).split(" ")
    // 注意:如果用户与其他任何人没有连接,则生成一个自连接
    // 所以顶点会被包含在Graph.fromEdgeTuples构建的图里面
    val edges = for {
      dstId <- dstIds
      if (dstId != "")
    } yield {
      (srcId.toLong, dstId.toLong)
    }
    if (edges.size > 0) edges else Array((srcId, srcId))
  }

  /**
    * 从文件内容中构建边元组
    *
    * @param contents
    * @return Array[(Long, Long)]
    */
  def make_edges(contents: String): Array[(Long, Long)] = {
    val lines = contents.split("\n")
    val unflat = for {
      line <- lines
    } yield {
      get_edges_from_line(line)
    }

    // 要传递给Graph.fromEdgeTuples函数一个元组类型的数组
    // 但现在是一个二维数组,这就需要调用flatten()函数来将其扁平化为一维数组
    val flat = unflat.flatten
    flat
  }

  /**
    * 从edge元组构建一个图对象,执行connectedComponents函数,返回String结果
    *
    * @param flat
    * @return String
    */
  def get_circles(flat: Array[(Long, Long)]): String = {
    val edges = sc.makeRDD(flat)
    val graph = Graph.fromEdgeTuples(edges, 1)
    val cc = graph.connectedComponents()
    cc.vertices.map(x => (x._2, Array(x._1)))
      .reduceByKey((a, b) => a ++ b)
      .values.map(_.mkString(" ")).collect.mkString(";")
  }

}
