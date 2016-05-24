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
import org.dbpedia.extraction.sources.XMLSource

import org.dbpedia.extraction.util.{ConfigUtils,Language}
import org.dbpedia.extraction.dump.extract.{Config,ConfigLoader}

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
    titles should have size 2
    titles should contain ("Apache Spark")
    titles should contain ("Apache Hadoop")
  }

  test("read titles from xml bz2 dump") {
    val titles = get_titles(xml_dump_bz2)
    titles should have size 2
    titles should contain ("Apache Spark")
    titles should contain ("Apache Hadoop")
  }

  test("create extraction job") {
    val config = ConfigUtils.loadConfig(config_file, "UTF-8")
    val jobs = new ConfigLoader(new Config(config)).getExtractionJobs()
  }

  def readQuadsInSequence() = {
    val config = ConfigUtils.loadConfig(config_file, "UTF-8")
    val jobs = new ConfigLoader(new Config(config)).getExtractionJobs()
    val language = Language("en")
    XMLSource.fromXML(XML.loadFile(xml_dump), language).flatMap {
      page => jobs.flatMap(_.getExtractor.apply(page)).map(SerializableQuad.apply)
    }.toList.sorted

  }

  test("extract in sequence") {
    val quads = readQuadsInSequence()
    quads should have size 18
  }

  def readQuadsInSpark() = {
    val config = ConfigUtils.loadConfig(config_file, "UTF-8")
    Util.readDumpToPageRdd(sc, xml_dump)
      .flatMap(DistExtractor(config, "en").extract(_))
      .collect().toList.sorted
  }
  test("extract in spark") {
    val quads = readQuadsInSpark()
    val quadsSeq = readQuadsInSequence()
    (quads.size) should equal (quadsSeq.size)
    for (i <- 0 to quads.size - 1) {
      (quads(i)) should equal (quadsSeq(i))
    }
  }

  def readQuadsInSparkWithBroadcast() = {
    val config = ConfigUtils.loadConfig(config_file, "UTF-8")
    val extractor = sc.broadcast(DistExtractor(config, "en"))
    Util.readDumpToPageRdd(sc, xml_dump)
      .flatMap(extractor.value.extract(_))
      .collect().toList.sorted
  }
  test("extract in spark with broacasted extractor") {
    val quads = readQuadsInSparkWithBroadcast()
    val quadsSeq = readQuadsInSequence()
    (quads.size) should equal (quadsSeq.size)
    for (i <- 0 to quads.size - 1) {
      (quads(i)) should equal (quadsSeq(i))
    }
  }
}
