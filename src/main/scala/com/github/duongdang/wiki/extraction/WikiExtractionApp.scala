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

import java.util.logging.{Level, Logger}

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.dbpedia.extraction.util.{ConfigUtils,Language}
import org.dbpedia.util.Exceptions

object WikiExtractApp {
  def main(args : Array[String]) {
    val conf = args(0)
    val lang = args(1)
    val input = args(2)
    val output = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("Extraction"))
    val config = ConfigUtils.loadConfig(conf, "UTF-8")
    val extractor = new DistExtractor(config, lang)
    Util.readDumpToPageRdd(sc, input)
      .flatMap { text =>
        try {
          extractor.extract(text)
        }
        catch {
          case ex: Exception =>
            Logger.getLogger(getClass.getName).log(Level.WARNING,
              "Processing error: %s. The page was: %s ..."
                .format(Exceptions.toString(ex, 200), text.take(100)))
            Seq()
        }
        }.map { quad => List(quad.language, quad.dataset, quad.subject,
          quad.predicate, quad.value, quad.context, quad.datatype).mkString("\t") }
          .saveAsTextFile(output)
  }
}
