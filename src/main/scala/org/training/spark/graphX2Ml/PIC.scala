package org.training.spark.graphX2Ml

import java.awt.image.BufferedImage
import java.awt.image.DataBufferInt
import java.awt.{Color, Image, color}
import java.io.File
import javax.imageio.ImageIO

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.training.spark.main.Singleton

object PIC {
  def main(args: Array[String]): Unit = {
    val sc = Singleton.getSingleton
    sc.setLogLevel("WARN")

    val im = ImageIO.read(new File("img/train/105053.jpg"))
    val ims = im.getScaledInstance(im.getWidth / 8, im.getHeight / 8,
      Image.SCALE_AREA_AVERAGING
    )

    val width = ims.getWidth(null)
    val height = ims.getHeight(null)
    val bi = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
    bi.getGraphics.drawImage(ims, 0, 0, null)
    val r = sc.makeRDD(bi.getData.getDataBuffer
      .asInstanceOf[DataBufferInt].getData)
      // 使用zipWithIndex给像素点进行编号，方便追踪与使用
      // 注意：执行完成之后需要catch一下，不然可能会执行两次zipWithIndex，
      // 而两次产生的编号可能不一致
      .zipWithIndex.cache
    // 计算所有像素两两间的相似度，使用笛卡儿积来处理，cartesian会产生笛卡儿积
    val g = Graph.fromEdges(r.cartesian(r).cache.map(x => {
      Edge(x._1._2, x._2._2, cosineSimilarity(toVec(x._1), toVec(x._2)))
    })
      // 过滤掉相似度小于0.5的边，这样做除了可以加速PIC算法外，最重要的是PIC要求所有相似度必须大于等于0
      .filter(e => e.attr > 0.5), 0.0).cache()

    val m = new PowerIterationClustering().run(g)
    val colors = Array(Color.white.getRGB, Color.black.getRGB)
    val bi2 = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
    m.assignments
      .map(a => (a.id / width, (a.id % width, colors(a.cluster))))
      .groupByKey
      .map(a => (a._1, a._2.toList.sortBy(_._1).map(_._2).toArray))
      .collect
      .foreach(x => bi2.setRGB(0, x._1.toInt, width, 1, x._2, 0, width))
    ImageIO.write(bi2, "PNG", new File("out.png"))

    sc.stop()
  }

  def toVec(a: Tuple2[Int, Long]): Array[Double] = {
    val c = new Color(a._1)
    Array[Double](c.getRed, c.getGreen, c.getBlue)
  }

  def cosineSimilarity(u: Array[Double], v: Array[Double]) = {
    val d = Math.sqrt(u.map(a => a * a).sum * v.map(a => a * a).sum)
    if (d == 0.0) 0.0 else u.zip(v).map(a => a._1 * a._2).sum / d
  }

}
