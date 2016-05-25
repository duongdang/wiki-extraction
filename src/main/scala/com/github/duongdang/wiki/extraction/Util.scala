// Copyright 2016 Duong Dang
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.github.duongdang.wiki.extraction

import scala.xml.XML
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.fs._
import scala.collection.mutable.ArrayBuffer

object Util {
  def listLanguages(fs: FileSystem, path: Path) = listDumps(fs, path).map(_._1)

  def readToLangPageRdd(sc: SparkContext, path: Path) : RDD[(String, String)] = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val files = listDumps(fs, path)
    files.map { p => readToPageRdd(sc, p._2).map{ page => (p._1, page) } }.reduce( _ union _)
  }

  private def readToPageRdd(sc: SparkContext, path: String) : RDD[String] = {
    sc.newAPIHadoopFile(
      path,
      classOf[WikiInputFormat],
      classOf[LongWritable],
      classOf[Text])
      .map(_._2.toString)
  }

  private def getAllFiles(fs: FileSystem, inputDir: Path) = {
    val files = fs.listFiles(inputDir, true)

    var fileArray = ArrayBuffer.empty[String]
    while (files.hasNext) {
      fileArray += files.next.getPath.toString
    }
    fileArray.toArray
  }

  private def listDumps(fs: FileSystem, path: Path) = {
    val files = { if (fs.isDirectory(path)) getAllFiles(fs, path) else Array(path.toString) }
    files.flatMap { fn =>
      val pattern = """([a-z]{2,})wiki""".r
      pattern findFirstIn fn match {
        case Some(pattern(lang)) => Some(lang, fn)
        case None => None
      }
    }
  }

}
