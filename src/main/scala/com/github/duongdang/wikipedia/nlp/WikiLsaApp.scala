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
package com.github.duongdang.wikipedia.nlp

import java.util.logging.{Level, Logger}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import org.apache.hadoop.fs._

object WikiLsaApp {
  def main(args : Array[String]) {
    val input = args(0)
    val output = args(1)
    val stopwordsFn = args(2)
    val numConcepts = args(3).toInt
    val numTermsCap = args(4).toInt

    val sc = new SparkContext(new SparkConf().setAppName("Extraction"))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val lsa = LatentSemanticAnalyzer(sc, input, stopwordsFn, numConcepts, numTermsCap)
    lsa.conceptRelevances().toDF().saveAsParquetFile(output)
  }
}
