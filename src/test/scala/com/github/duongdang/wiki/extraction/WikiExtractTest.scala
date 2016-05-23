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

import org.dbpedia.extraction.util.ConfigUtils
import org.dbpedia.extraction.dump.extract.{Config,ConfigLoader}

import org.dbpedia.extraction.util.Language
import org.dbpedia.extraction.sources.XMLSource

@RunWith(classOf[JUnitRunner])
class WikiExtractTest extends FunSuite with SharedSparkContext with Matchers {
  val xml_dump = "src/test/resources/dumps/spark_hadoop_articles.xml"
  val xml_dump_bz2 = "src/test/resources/dumps/enwiki/20160501/enwiki-20160501-pages-articles.xml.bz2"
  val config_file = "src/test/resources/dbpedia/config.properties"

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
    Util.readDumpToPageRdd(sc, in)
      .map { s =>
      val xml = XML.loadString(s)
      val id = (xml \ "id").text.toDouble
      val title = (xml \ "title").text
      val text = (xml \ "revision" \ "text").text.replaceAll("\\W", " ")
      (title)
    }
      .collect()
  }

  test("read titles from xml dump") {
    val titles = get_titles(xml_dump)
    titles should contain ("Apache Spark")
    titles should contain ("Apache Hadoop")
  }

  test("read titles from xml bz2 dump") {
    val titles = get_titles(xml_dump_bz2)
    titles should contain ("Apache Spark")
    titles should contain ("Apache Hadoop")
  }

  test("create extraction job") {
    val config = ConfigUtils.loadConfig(config_file, "UTF-8")
    val jobs = new ConfigLoader(new Config(config)).getExtractionJobs()
  }

  test("extract in sequence") {
    val config = ConfigUtils.loadConfig(config_file, "UTF-8")
    val jobs = new ConfigLoader(new Config(config)).getExtractionJobs()
    val language = Language("en")
    val xml = XMLSource.fromXML(XML.loadFile(xml_dump), language).head
    val quads = jobs.flatMap(_.getExtractor.apply(xml))
    quads should have size 9
  }

  test("extract in spark") {
    val config = ConfigUtils.loadConfig(config_file, "UTF-8")
    val quads = Util.readDumpToPageRdd(sc, xml_dump_bz2).flatMap
    { text =>
      val jobs = new ConfigLoader(new Config(config)).getExtractionJobs()
      val language = Language("en")
      val xml = XMLSource.fromXML(XML.loadString("<mediawiki>" + text + "</mediawiki>"), language).head
      jobs.flatMap(_.getExtractor.apply(xml))
    }.collect()
    quads should have size 9
  }
}
