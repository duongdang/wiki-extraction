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

import org.dbpedia.extraction.sources.XMLSource
import org.dbpedia.extraction.util.Language
import scala.xml.XML
import java.io.Serializable

class DistExtractor(config: Properties, lang: String) extends Serializable{
  @transient private val language = Language(lang)
  @transient private val jobs = new DistConfigLoader(new DistConfig(config, language)).getExtractionJobs()
  def extract(text : String) = {
    val xml = XMLSource.fromXML(XML.loadString("<mediawiki>" + text + "</mediawiki>"), language).head
    jobs.flatMap(_.getExtractor.apply(xml)).map(SerializableQuad.apply)
  }
}

object DistExtractor {
  def apply(config: Properties, lang: String) : DistExtractor =  new DistExtractor(config, lang)
}
