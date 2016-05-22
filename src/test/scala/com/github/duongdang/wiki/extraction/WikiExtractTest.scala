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

import com.holdenkarau.spark.testing.{SharedSparkContext}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite,Matchers}

@RunWith(classOf[JUnitRunner])
class WikiExtractTest extends FunSuite with SharedSparkContext with Matchers {
  val xml_dump = "src/test/resources/dumps/spark_hadoop_articles.xml"
  val xml_dump_bz2 = "src/test/resources/dumps/spark_hadoop_articles.xml.bz2"

  test("really simple transformation") {
    val col = sc.parallelize(0 to 100 by 5)
    val colCount = col.count
    colCount should equal (21)
  }

  test("read xml by line") {
    val rdd = sc.textFile(xml_dump)
    val num_lines = rdd.count
    num_lines should equal (904)
  }

  def get_titles(in : String) = {
    Util.readDumpToPageRdd(sc, xml_dump)
      .map { s =>
      val xml = XML.loadString(s)
      val id = (xml \ "id").text.toDouble
      val title = (xml \ "title").text
      val text = (xml \ "revision" \ "text").text.replaceAll("\\W", " ")
      (title)
    }
      .collect()
  }

  test("read xml by record") {
    for (fn <- List(xml_dump, xml_dump_bz2)){
      val titles = get_titles(fn)
      titles should contain ("Apache Spark")
      titles should contain ("Apache Hadoop")
    }
  }
}
