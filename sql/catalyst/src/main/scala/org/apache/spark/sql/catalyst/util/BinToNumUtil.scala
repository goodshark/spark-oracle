
package org.apache.spark.sql.catalyst.util

/**
  * Created by zhongdg1 on 2017/7/27.
  */
object BinToNumUtil {

  def binToNum(exprs: Long*): Int = {

    exprs.map(_.toInt).reverse.zipWithIndex.foldLeft[Int](0)((result, current) => {
      result + {
        current._1 match {
          case 0 => 0
          case 1 => scala.math.pow(2, current._2).toInt
        }
      }
    })

  }

  def main(args: Array[String]): Unit = {
//    print(binToNum(Seq(1, 1, 0)))
  }
}
