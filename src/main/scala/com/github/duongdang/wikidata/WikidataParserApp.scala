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
package com.github.duongdang.wikidata

import java.util.logging.{Level, Logger}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.fs._

object WikidataParserApp {
  def main(args : Array[String]) {
    val input = args(0)
    val output = args(1)

    val sc = new SparkContext(new SparkConf().setAppName("WikidataParserApp"))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rdd = sc.textFile(input)

    rdd.flatMap { line =>
      try {
        WikidataLabel.fromText(line)
      }
      catch {
        case ex: Exception =>
          Logger.getLogger("WikidataParserApp").log(Level.WARNING,
            "Processing error: %s. The page was: %s ..."
              .format(ex, line.take(100)))
          Seq()
      }
    }.toDF.saveAsParquetFile((new Path(output, "wikidatalabels")).toString)

    rdd.flatMap { line =>
      try {
        WikidataSitelink.fromText(line)
      }
      catch {
        case ex: Exception =>
          Logger.getLogger("WikidataParserApp").log(Level.WARNING,
            "Processing error: %s. The page was: %s ..."
              .format(ex, line.take(100)))
          Seq()
      }
    }.toDF.saveAsParquetFile((new Path(output, "wikidatasitelinks")).toString)

    rdd.flatMap { line =>
      try {
        WikidataClaim.fromText(line)
      }
      catch {
        case ex: Exception =>
          Logger.getLogger("WikidataParserApp").log(Level.WARNING,
            "Processing error: %s. The page was: %s ..."
              .format(ex, line.take(100)))
          Seq()
      }
    }.toDF.saveAsParquetFile((new Path(output, "wikidataclaims")).toString)
  }
}
