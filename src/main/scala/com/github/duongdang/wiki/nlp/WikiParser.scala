/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.github.duongdang.wiki.nlp

import java.io.{FileOutputStream, PrintStream}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs._

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import java.util.logging.{Level, Logger}

import com.github.duongdang.wiki.io.Util
import org.apache.spark.rdd.RDD

import org.tartarus.snowball.ext._
import org.tartarus.snowball.SnowballProgram

import scala.xml.XML

object WikiParser {
  /**
   * Returns a document-term matrix where each element is the TF-IDF of the row's document and
   * the column's term.
   */
  val logger = Logger.getLogger("WikiParser")

  /**
   * Returns
   *   - an RDD of rows of the document-term matrix,
   *   - a mapping of column indices to terms,
   *   - a mapping of row IDs to document titles.
   *   - idfs values
   */
  def parse(sc: SparkContext, input: String, stopwordsFn: String, numTermsCap: Int)
      : (RDD[Vector], Map[Int, String], Map[Long, String], Map[String, Double]) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val langs = Util.listLanguages(fs, new Path(input))
    if (langs.size != 1) {
      throw new RuntimeException("Only one language is expected. Found in input: %s".format(langs.mkString(",")))
    }
    val lang = langs(0)
    val stopWords = sc.broadcast(WikiParser.loadStopWords(stopwordsFn)).value
    val pages = Util.readToLangPageRdd(sc, new Path(input)).map(_._2)
    val plainText = pages.filter(_ != null).flatMap(wikiXmlToPlainText)
    val lemmatized = plainText.mapPartitions { iter =>
      val stemmer: SnowballProgram = lang match {
        case "da" => new DanishStemmer()
        case "nl" => new DutchStemmer()
        case "en" => new EnglishStemmer()
        case "fi" => new FinnishStemmer()
        case "fr" => new FrenchStemmer()
        case "de" => new GermanStemmer()
        case "hu" => new HungarianStemmer()
        case "it" => new ItalianStemmer()
        case "no" => new NorwegianStemmer()
        case "pt" => new PortugueseStemmer()
        case "ro" => new RomanianStemmer()
        case "ru" => new RussianStemmer()
        case "es" => new SpanishStemmer()
        case "sk" => new SwedishStemmer()
        case "tr" => new TurkishStemmer()
        case _ => new EnglishStemmer()
      }
      iter.map{ case (title, contents) => (title, plainTextToLemmas(contents, stopWords, stemmer))
      }
    }

    val filtered = lemmatized.filter(_._2.size > 1)

    documentTermMatrix(filtered, stopWords, numTermsCap, sc)
  }


  def documentTermMatrix(docs: RDD[(String, Seq[String])],
    stopWords: Set[String], numTerms: Int,
    sc: SparkContext):
      (RDD[Vector], Map[Int, String], Map[Long, String], Map[String, Double]) = {
    val docTermFreqs = docs.mapValues(terms => {
      val termFreqsInDoc = terms.foldLeft(new HashMap[String, Int]()) {
        (map, term) => map += term -> (map.getOrElse(term, 0) + 1)
      }
      termFreqsInDoc
    })

    docTermFreqs.cache()
    val docIds = docTermFreqs.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()

    val docFreqs = documentFrequenciesDistributed(docTermFreqs.map(_._2), numTerms)
    println("Number of terms: " + docFreqs.size)
    // saveDocFreqs("docfreqs.tsv", docFreqs)

    val numDocs = docIds.size

    val idfs = inverseDocumentFrequencies(docFreqs, numDocs)

    // Maps terms to their indices in the vector
    val idTerms = idfs.keys.zipWithIndex.toMap
    val termIds = idTerms.map(_.swap)

    val bIdfs = sc.broadcast(idfs).value
    val bIdTerms = sc.broadcast(idTerms).value

    val vecs = docTermFreqs.map(_._2).map(termFreqs => {
      val docTotalTerms = termFreqs.values.sum
      val termScores = termFreqs.filter {
        case (term, freq) => bIdTerms.contains(term)
      }.map{
        case (term, freq) => (bIdTerms(term), bIdfs(term) * termFreqs(term) / docTotalTerms)
      }.toSeq
      Vectors.sparse(bIdTerms.size, termScores)
    })
    (vecs, termIds, docIds, idfs)
  }

  def documentFrequencies(docTermFreqs: RDD[HashMap[String, Int]]): HashMap[String, Int] = {
    val zero = new HashMap[String, Int]()
    def merge(dfs: HashMap[String, Int], tfs: HashMap[String, Int])
      : HashMap[String, Int] = {
      tfs.keySet.foreach { term =>
        dfs += term -> (dfs.getOrElse(term, 0) + 1)
      }
      dfs
    }
    def comb(dfs1: HashMap[String, Int], dfs2: HashMap[String, Int])
      : HashMap[String, Int] = {
      for ((term, count) <- dfs2) {
        dfs1 += term -> (dfs1.getOrElse(term, 0) + count)
      }
      dfs1
    }
    docTermFreqs.aggregate(zero)(merge, comb)
  }

  def documentFrequenciesDistributed(docTermFreqs: RDD[HashMap[String, Int]], numTerms: Int)
      : Array[(String, Int)] = {
    val docFreqs = docTermFreqs.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _, 15)
    val ordering = Ordering.by[(String, Int), Int](_._2)
    docFreqs.top(numTerms)(ordering)
  }

  def trimLeastFrequent(freqs: Map[String, Int], numToKeep: Int): Map[String, Int] = {
    freqs.toArray.sortBy(_._2).take(math.min(numToKeep, freqs.size)).toMap
  }

  def inverseDocumentFrequencies(docFreqs: Array[(String, Int)], numDocs: Int)
    : Map[String, Double] = {
    docFreqs.map{ case (term, count) => (term, math.log(numDocs.toDouble / count))}.toMap
  }

  /**
   * Returns a (title, content) pair
   */
  def wikiXmlToPlainText(s: String): Option[(String, String)] = {
    val xml = XML.loadString(s)
    val title = (xml \ "title").text
    val text = (xml \ "revision" \ "text").text

    if (text.isEmpty || title.contains("disambiguation") || title.contains(":")) {
      logger.log(Level.WARNING, "Ignoring page %s".format(title))
        // "Ignoring page %s. isEmpty=%b, isArticle=%b, isRedirect=%b".format(page.getTitle, page.isEmpty, page.isArticle, page.isRedirect))
      None
    } else {
      Some((title, text))
    }
  }

  def plainTextToLemmas(text: String, stopWords: Set[String], stemmer: SnowballProgram)
    : Seq[String] = {
    text
      .split("[^\\w-]")
      .map(_.toLowerCase)
      .filter(_.size > 2)
      .filter(isOnlyLetters)
      .map { w =>
      stemmer.setCurrent(w)
      stemmer.stem()
      stemmer.getCurrent
    }
      .filter(!stopWords.contains(_))
  }

  def isOnlyLetters(str: String): Boolean = {
    // While loop for high performance
    var i = 0
    while (i < str.length) {
      if (!Character.isLetter(str.charAt(i))) {
        return false
      }
      i += 1
    }
    true
  }

  def loadStopWords(path: String) = scala.io.Source.fromFile(path).getLines().toSet
}
