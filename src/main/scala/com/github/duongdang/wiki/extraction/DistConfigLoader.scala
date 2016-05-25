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

class DistConfigLoader(config: DistConfig) {
  private val logger = Logger.getLogger(classOf[DistConfigLoader].getName)

  def getExtractors(): Traversable[RootExtractor] =
  {
    config.extractorClasses.view.map(e => createExtractor(e._1, e._2))
  }

  private def createExtractor(lang : Language, extractorClasses: Seq[Class[_ <: Extractor[_]]]) : RootExtractor =
  {
    //Extraction Context
    val context = new DumpExtractionContext
    {
      lazy val _ontology = new OntologyReader().read(XMLSource.fromXML(config.ontologyXML, Language.Mappings))
      def ontology = _ontology

      def commonsSource : Source = null

      def language = lang

      private lazy val _mappingPageSource = XMLSource.fromXML(config.mappingXML, Language.Mappings)
      def mappingPageSource = _mappingPageSource

      private lazy val _mappings = MappingsLoader.load(this)
      def mappings = _mappings

      def articlesSource: Source = null

      def redirects = new Redirects(Map())

      def disambiguations = new Disambiguations(Set[Long]())
    }

    //Extractors
    val extractor = CompositeParseExtractor.load(extractorClasses, context)
    new RootExtractor(extractor)
  }
}
