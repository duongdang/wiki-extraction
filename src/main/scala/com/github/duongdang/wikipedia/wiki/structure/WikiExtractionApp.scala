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
package com.github.duongdang.wikipedia.structure
import com.github.duongdang.wikipedia.io.Util

import java.util.logging.{Level, Logger}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import org.dbpedia.extraction.util.{ConfigUtils,Language}
import org.dbpedia.util.Exceptions

import org.apache.hadoop.fs._

object WikiExtractApp {
  def main(args : Array[String]) {
    val conf = args(0)
    val input = args(1)
    val output = args(2)

    val sc = new SparkContext(new SparkConf().setAppName("Extraction"))
    val config = ConfigUtils.loadConfig(conf, "UTF-8")

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val langs = Util.listLanguages(fs, new Path(input))
    println("Input: %s. Creating extractors for %d languages: %s".format(
      input, langs.size, langs.mkString(",")))
    assert(langs.size > 0)

    val extractors = langs.map {
      lang => (lang -> new DistExtractor(config, lang)) }.toMap

    Util.readToLangPageRdd(sc, new Path(input))
      .flatMap { case (lang, text) =>
        try {
          extractors.get(lang).get.extract(text)
        }
        catch {
          case ex: Exception =>
            Logger.getLogger("WikiExtractApp").log(Level.WARNING,
              "Processing error: %s. The page was: %s ..."
                .format(Exceptions.toString(ex, 200), text.take(100)))
            Seq()
        }
        }.toDF.saveAsParquetFile(output)
  }
}
