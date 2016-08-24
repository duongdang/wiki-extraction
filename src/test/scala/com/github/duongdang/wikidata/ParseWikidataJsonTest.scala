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

import com.holdenkarau.spark.testing.{SharedSparkContext}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite,Matchers}
import org.dbpedia.extraction.sources.XMLSource

import org.dbpedia.extraction.util.{ConfigUtils,Language}
import org.dbpedia.extraction.dump.extract.{Config,ConfigLoader}
import org.apache.hadoop.fs._


@RunWith(classOf[JUnitRunner])
class ParseWikidataJsonTest extends FunSuite with Matchers {
  val paris_json = "src/test/resources/wikidata/Q90.json"

  test("test label parser") {
    val jsonString = scala.io.Source.fromFile(paris_json).mkString
    val labels = WikidataLabel.fromText(jsonString)
    (labels.size) should equal (249)
    labels should contain (WikidataLabel("Q90", "item", "en", "Paris", "capital city of France"))
  }

  test("test sitelink parser") {
    val jsonString = scala.io.Source.fromFile(paris_json).mkString
    val links = WikidataSiteLink.fromText(jsonString)
    links should contain (WikidataSiteLink("Q90", "item", "enwiki", "Paris", "https://en.wikipedia.org/wiki/Paris"))
  }

  test("test claim parser") {
    val jsonString = scala.io.Source.fromFile(paris_json).mkString
    val claims = WikidataClaim.fromText(jsonString)
    claims.foreach(println)
  }

}
