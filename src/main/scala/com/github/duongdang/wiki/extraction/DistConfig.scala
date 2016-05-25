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

import java.io.Serializable
import java.util.Properties
import java.io.File

import org.dbpedia.extraction.mappings.Extractor

import org.dbpedia.extraction.util.{ExtractorUtils, Language}
import org.dbpedia.extraction.wikiparser.Namespace
import scala.collection.Map
import scala.xml.XML

class DistConfig(props: Properties, lang: String) extends Serializable {
  @transient val language = Language(lang)
  val ontologyXML = XML.loadFile(props.getProperty("ontology"))

  @transient private val namespace = Namespace.mappings(language)
  val mappingXML = XML.loadFile(new File(props.getProperty("mappings"),
    namespace.name(Language.Mappings).replace(' ','_')+".xml"))

  val extractorClasses = loadExtractorClasses()
  private def loadExtractorClasses() : Map[Language, Seq[Class[_ <: Extractor[_]]]] = {
    val languages = Seq(language)

    ExtractorUtils.loadExtractorsMapFromConfig(languages, props)
  }
}
