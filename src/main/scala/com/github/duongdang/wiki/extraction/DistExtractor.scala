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
import java.io.{File,IOException}

import org.dbpedia.extraction.ontology.io.OntologyReader
import org.dbpedia.extraction.dump.extract.{DumpExtractionContext}
import org.dbpedia.extraction.sources.{XMLSource, Source}
import org.dbpedia.extraction.util.ConfigUtils.{getStrings}
import org.dbpedia.extraction.util.{ExtractorUtils, Language}
import org.dbpedia.extraction.wikiparser.{Namespace}
import org.dbpedia.extraction.mappings.{CompositeParseExtractor,MappingsLoader,
  Disambiguations,Redirects,RootExtractor}
import java.io.ObjectInputStream

import scala.collection.immutable.Map
import scala.xml.XML

class DistExtractor(props: Properties, lang: String) extends Serializable {
  private val ontologyXML = XML.loadFile(props.getProperty("ontology"))
  private val mappingXML = {
    val namespace = Namespace.mappings(Language(lang))
    XML.loadFile(new File(props.getProperty("mappings"),
    namespace.name(Language.Mappings).replace(' ','_')+".xml"))
  }

  @transient private var impl : RootExtractor = createExtractor()

  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    impl = createExtractor()
  }

  def extract(text : String) = {
    val xml = XMLSource.fromXML(XML.loadString("<mediawiki>" + text + "</mediawiki>"), Language(lang)).head
    impl.apply(xml).map(SerializableQuad.apply)
  }

  def createExtractor() = {
    val extractorClasses = ExtractorUtils.loadExtractorClassSeq(
      getStrings(props, "extractors", ',', false))

    val context = new DumpExtractionContext {
      lazy val _ontology = new OntologyReader().read(XMLSource.fromXML(ontologyXML, Language.Mappings))
      lazy val _mappingPageSource =  XMLSource.fromXML(mappingXML, Language.Mappings)
      lazy val _language = Language(lang)
      lazy val _redirects = new Redirects(Map())
      lazy val _mappings = MappingsLoader.load(this)
      def ontology = _ontology
      def commonsSource : Source = null
      def language = _language
      def mappingPageSource = _mappingPageSource

      def mappings = _mappings
      def articlesSource: Source = null
      def redirects = _redirects
      def disambiguations = new Disambiguations(Set[Long]())
    }
    new RootExtractor(CompositeParseExtractor.load(extractorClasses, context))
  }
}
