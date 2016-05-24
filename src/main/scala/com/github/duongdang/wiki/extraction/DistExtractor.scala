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
import java.util.Properties
import org.dbpedia.extraction.dump.extract.{Config,ConfigLoader}
import org.dbpedia.extraction.mappings.RootExtractor
import org.dbpedia.extraction.sources.XMLSource
import org.dbpedia.extraction.util.{ConfigUtils, Language}
import org.dbpedia.extraction.util.ConfigUtils.{getValue,getStrings}
import scala.xml.XML
import java.io.Serializable
import org.apache.hadoop.fs.Path
import java.io.File

class DistExtractor(props: Properties, lang: String) extends Serializable{
  @transient private val language = Language(lang)
  @transient private val extractors = createExtractors()
  def extract(text : String) = {
    val xml = XMLSource.fromXML(XML.loadString("<mediawiki>" + text + "</mediawiki>"), language).head
    extractors.flatMap(_.apply(xml)).map(SerializableQuad.apply)
  }


  private class DistConfig extends Config(props) {
    override lazy val dumpDir: File = null
    override lazy val ontologyFile: File = null
    override lazy val mappingsDir: File = null
  }


  private def createExtractors() : Traversable[RootExtractor] = {
    val config = new DistConfig()
  }

  private def createExtractor(
}

object DistExtractor {
  def apply(props: Properties, lang: String) : DistExtractor =  new DistExtractor(props, lang)
}
