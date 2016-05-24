package com.github.duongdang.wiki.extraction

import org.dbpedia.extraction.destinations.formatters.UriPolicy.parseFormats
import org.dbpedia.extraction.mappings.Extractor
import java.util.Properties
import java.io.File
import org.dbpedia.extraction.wikiparser.Namespace
import scala.collection.Map
import org.dbpedia.extraction.util.{ConfigUtils, ExtractorUtils, Language}
import org.dbpedia.extraction.util.ConfigUtils.{getValue,getStrings}


class DistConfig(props: Properties, language: Language)
{
  val ontologyFile = getValue(props, "ontology", false)(new File(_))
  val mappingsDir = getValue(props, "mappings", false)(new File(_))
  val extractorClasses = loadExtractorClasses()
  private def loadExtractorClasses() : Map[Language, Seq[Class[_ <: Extractor[_]]]] =
  {
    val languages = Seq(language)

    ExtractorUtils.loadExtractorsMapFromConfig(languages, props)
  }
}
