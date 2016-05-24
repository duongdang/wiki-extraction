package com.github.duongdang.wiki.extraction

import org.dbpedia.extraction.dump.extract.{ExtractionJob, DumpExtractionContext, Extraction}
import org.dbpedia.extraction.destinations._
import org.dbpedia.extraction.mappings._
import org.dbpedia.extraction.ontology.io.OntologyReader
import org.dbpedia.extraction.sources.{WikiPage, XMLSource, WikiSource, Source}
import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.dump.download.Download
import org.dbpedia.extraction.util.{Language, Finder, ExtractorUtils}
import org.dbpedia.extraction.util.RichFile.wrapFile
import scala.collection.mutable.{ArrayBuffer,HashMap}
import java.io._
import java.net.URL
import scala.io.Codec.UTF8
import java.util.logging.Logger
import org.dbpedia.extraction.util.IOUtils

/**
  * Loads the dump extraction configuration.
  *
  * TODO: clean up. The relations between the objects, classes and methods have become a bit chaotic.
  * There is no clean separation of concerns.
  *
  * TODO: get rid of all config file parsers, use Spring
  */
class DistConfigLoader(config: DistConfig)
{
  private val logger = Logger.getLogger(classOf[DistConfigLoader].getName)

  /**
    * Loads the configuration and creates extraction jobs for all configured languages.
    *
    * @param configFile The configuration file
    * @return Non-strict Traversable over all configured extraction jobs i.e. an extractions job will not be created until it is explicitly requested.
    */
  def getExtractors(): Traversable[RootExtractor] =
  {
    // Create a non-strict view of the extraction jobs
    // non-strict because we want to create the extraction job when it is needed, not earlier
    config.extractorClasses.view.map(e => createExtractor(e._1, e._2))
  }

  /**
    * Creates ab extraction job for a specific language.
    */
  private def createExtractor(lang : Language, extractorClasses: Seq[Class[_ <: Extractor[_]]]) : RootExtractor =
  {
    //Extraction Context
    val context = new DumpExtractionContext
    {
      def ontology = _ontology

      def commonsSource : Source = null

      def language = lang

      private lazy val _mappingPageSource =
      {
        val namespace = Namespace.mappings(language)

        if (config.mappingsDir != null && config.mappingsDir.isDirectory)
        {
          val file = new File(config.mappingsDir, namespace.name(Language.Mappings).replace(' ','_')+".xml")
          XMLSource.fromFile(file, Language.Mappings)
        }
        else
        {
          val namespaces = Set(namespace)
          val url = new URL(Language.Mappings.apiUri)
          WikiSource.fromNamespaces(namespaces,url,Language.Mappings)
        }
      }

      def mappingPageSource : Traversable[WikiPage] = _mappingPageSource

      private lazy val _mappings =
      {
        MappingsLoader.load(this)
      }
      def mappings : Mappings = _mappings

      def articlesSource: Source = null

      def redirects = new Redirects(Map())

      def disambiguations = new Disambiguations(Set[Long]())
    }

    //Extractors
    val extractor = CompositeParseExtractor.load(extractorClasses, context)
    new RootExtractor(extractor)
  }

  //language-independent val
  private lazy val _ontology =
  {
    val ontologySource = if (config.ontologyFile != null && config.ontologyFile.isFile)
    {
      XMLSource.fromFile(config.ontologyFile, Language.Mappings)
    }
    else
    {
      val namespaces = Set(Namespace.OntologyClass, Namespace.OntologyProperty)
      val url = new URL(Language.Mappings.apiUri)
      val language = Language.Mappings
      WikiSource.fromNamespaces(namespaces, url, language)
    }

    new OntologyReader().read(ontologySource)
  }

}