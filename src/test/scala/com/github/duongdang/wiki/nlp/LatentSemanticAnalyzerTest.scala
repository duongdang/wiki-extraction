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
package com.github.duongdang.wiki.nlp

import com.holdenkarau.spark.testing.{SharedSparkContext}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite,Matchers}

@RunWith(classOf[JUnitRunner])
class LatentSemanticAnalyzerTest extends FunSuite with SharedSparkContext with Matchers {
  val xml_dump = "src/test/resources/nlp/enwiki_sports_music.xml"
  val stopwords_file = "src/main/resources/nlp/stopwords.txt"

  test("create ls analyzer") {
    val lsa = LatentSemanticAnalyzer(sc, xml_dump, stopwords_file, 10, 50000)

    println("Singular values: " + lsa.svd.s)

    val topConceptTerms = lsa.topTermsInTopConcepts(10, 10)
    val topConceptDocs = lsa.topDocsInTopConcepts(10, 10)
    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", "))
      println("Concept docs: " + docs.map(_._1).mkString(", "))
      println()
    }
  }

}
