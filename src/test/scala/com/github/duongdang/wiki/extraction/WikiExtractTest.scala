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
import org.apache.hadoop.fs._

@RunWith(classOf[JUnitRunner])
class WikiExtractTest extends FunSuite with SharedSparkContext with Matchers {
  val xml_dump_dir = "src/test/resources/dumps/xml"
  val xml_dump = "src/test/resources/dumps/xml/enwiki_spark_hadoop_articles.xml"
  val bz2_dump = "src/test/resources/dumps/bz2/enwiki_spark_hadoop_articles.xml.bz2"
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
    Util.readToLangPageRdd(sc, new Path(in))
      .map { p =>
      val s = p._2
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
    val titles = get_titles(bz2_dump)
    titles should have size 2
    titles should contain ("Apache Spark")
    titles should contain ("Apache Hadoop")
  }

  test("create extraction job") {
    val config = ConfigUtils.loadConfig(config_file, "UTF-8")
    val extractor = new DistExtractor(config, "en")
  }

  def readQuadsInSequence() = {
    val config = ConfigUtils.loadConfig(config_file, "UTF-8")
    val extractor = new DistExtractor(config, "en")
    val language = Language("en")
    XMLSource.fromXML(XML.loadFile(xml_dump), language)
      .flatMap(extractor.createExtractor().apply(_)).map(SerializableQuad.apply)
      .toList.sorted
  }

  test("extract in sequence") {
    val quads = readQuadsInSequence()
    quads should have size 18
  }

  def readQuadsInSpark() = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val config = ConfigUtils.loadConfig(config_file, "UTF-8")

    val extractors = Util.listLanguages(fs, new Path(xml_dump_dir)).map {
      lang => (lang -> new DistExtractor(config, lang)) }.toMap

    Util.readToLangPageRdd(sc, new Path(xml_dump))
      .flatMap{case (lang, text) => extractors.get(lang).get.extract(text) }
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
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val config = ConfigUtils.loadConfig(config_file, "UTF-8")

    val extractors = Util.listLanguages(fs, new Path(xml_dump)).map {
      lang => (lang -> new DistExtractor(config, lang)) }.toMap

    val extractorsBC = sc.broadcast(extractors)

    Util.readToLangPageRdd(sc, new Path(xml_dump))
      .flatMap{case (lang, text) => extractorsBC.value.get(lang).get.extract(text) }
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

  test("list languages") {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val langs = Util.listLanguages(fs, new Path(xml_dump_dir))
    langs should contain ("war")
    langs should contain ("en")
  }
}
