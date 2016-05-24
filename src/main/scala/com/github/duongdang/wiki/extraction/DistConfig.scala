package com.github.duongdang.wiki.extraction

import org.dbpedia.extraction.destinations.formatters.UriPolicy.parseFormats
import org.dbpedia.extraction.mappings.Extractor
import java.util.Properties
import java.io.File
import org.dbpedia.extraction.wikiparser.Namespace
import scala.collection.Map
import org.dbpedia.extraction.util.{ConfigUtils, ExtractorUtils, Language}
import org.dbpedia.extraction.util.ConfigUtils.{getValue,getStrings}


class DistConfig(props: Properties)
{
  // TODO: get rid of all config file parsers, use Spring

  /**
   * Dump directory
   * Note: This is lazy to defer initialization until actually called (eg. this class is not used
   * directly in the distributed extraction framework - DistConfig.ExtractionConfig extends Config
   * and overrides this val to null because it is not needed)
   */
  lazy val dumpDir = getValue(props, "base-dir", true){
    x =>
      val dir = new File(x)
      if (! dir.exists) throw error("dir "+dir+" does not exist")
      dir
  }

  val requireComplete = props.getProperty("require-download-complete", "false").toBoolean

  // Watch out, this could be a regex
  val source = props.getProperty("source", "pages-articles.xml.bz2")
  val disambiguations = props.getProperty("disambiguations", "page_props.sql.gz")

  val wikiName = props.getProperty("wikiName", "wiki")

  val parser = props.getProperty("parser", "simple")

  /**
   * Local ontology file, downloaded for speed and reproducibility
   * Note: This is lazy to defer initialization until actually called (eg. this class is not used
   * directly in the distributed extraction framework - DistConfig.ExtractionConfig extends Config
   * and overrides this val to null because it is not needed)
   */
  lazy val ontologyFile = getValue(props, "ontology", false)(new File(_))

  /**
   * Local mappings files, downloaded for speed and reproducibility
   * Note: This is lazy to defer initialization until actually called (eg. this class is not used
   * directly in the distributed extraction framework - DistConfig.ExtractionConfig extends Config
   * and overrides this val to null because it is not needed)
   */
  lazy val mappingsDir = getValue(props, "mappings", false)(new File(_))

  val formats = parseFormats(props, "uri-policy", "format")

  val extractorClasses = loadExtractorClasses()

  val namespaces = loadNamespaces()

  private def loadNamespaces(): Set[Namespace] = {
    val names = getStrings(props, "namespaces", ',', false)
    if (names.isEmpty) Set(Namespace.Main, Namespace.File, Namespace.Category, Namespace.Template, Namespace.WikidataProperty)
    // Special case for namespace "Main" - its Wikipedia name is the empty string ""
    else names.map(name => if (name.toLowerCase(Language.English.locale) == "main") Namespace.Main else Namespace(Language.English, name)).toSet
  }

  /**
   * Loads the extractors classes from the configuration.
   * Loads only the languages defined in the languages property
   *
   * @return A Map which contains the extractor classes for each language
   */
  private def loadExtractorClasses() : Map[Language, Seq[Class[_ <: Extractor[_]]]] =
  {
    val languages = ConfigUtils.parseLanguages(dumpDir,getStrings(props, "languages", ',', false))

    ExtractorUtils.loadExtractorsMapFromConfig(languages, props)
  }

  private def error(message: String, cause: Throwable = null): IllegalArgumentException = {
    new IllegalArgumentException(message, cause)
  }

}
