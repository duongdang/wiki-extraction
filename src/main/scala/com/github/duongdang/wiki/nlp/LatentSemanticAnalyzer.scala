/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.github.duongdang.wiki.nlp

import com.github.duongdang.wiki.io.Util
import breeze.linalg.{DenseMatrix => BDenseMatrix, DenseVector => BDenseVector,
SparseVector => BSparseVector}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import java.io.Serializable

case class SemanticData(
  data_type: String,
  ref: String,
  key: String,
  score: Double
) extends Serializable

class LatentSemanticAnalyzer(@transient sc: SparkContext, input: String, stopwords_fn: String,
  numConcepts: Int = 1000,
  numTermsCap: Int = 50000) extends Serializable {
  import LatentSemanticAnalyzer._
  val stopWords = sc.broadcast(ParseWikipedia.loadStopWords(stopwords_fn)).value

  val (termDocMatrix, termIds, docIds, idfs) = preprocess()
  termDocMatrix.cache()
  val mat = new RowMatrix(termDocMatrix)
  val svd = mat.computeSVD(numConcepts, computeU=true)

  val vArray = sc.broadcast(svd.V.toArray).value
  val sArray = sc.broadcast(svd.s.toArray).value
  val vsArray = {
    val VS = multiplyByDiagonalMatrix(svd.V, svd.s)
    val normalizedVS = rowsNormalized(VS)
    sc.broadcast(normalizedVS.toArray).value
  }

  val numTerms = svd.V.numRows
  val distTermIds = sc.broadcast(termIds).value
  val distNumConcepts = sc.broadcast(numConcepts).value
  val distDocIds = sc.broadcast(docIds).value

  def conceptRelevances() = {
    val singularVals = sArray.zipWithIndex.map {
      case(v, i) => SemanticData("concept_strength", i.toString, null, v) }

    val termToConcept = sc.parallelize(List.range(0, numConcepts)).flatMap{ conceptId =>
      val offs = conceptId * numConcepts
      vArray.slice(offs, offs + numConcepts).zipWithIndex.map{
        case (score, id) =>
          SemanticData("term_to_concept", conceptId.toString, distTermIds(id), score)
      }
    }

    val docToConcept = List.range(0, numConcepts).map { conceptId =>
      val docWeights = svd.U.rows.map(_.toArray(conceptId)).zipWithUniqueId
      docWeights.map{case (score, id) => SemanticData("doc_to_concept", conceptId.toString, distDocIds(id), score)}
    }.reduce(_ union _)

    val termToTerm = sc.parallelize(List.range(0, numTerms)).flatMap { termId =>
      val offs = termId * numConcepts
      val termRowVec = new BDenseVector[Double](vsArray.slice(offs, offs + numConcepts))
      // Compute scores against every term
      val VS = new BDenseMatrix(numTerms, numConcepts, vsArray)
      val termScores = (VS * termRowVec).toArray.zipWithIndex
      termScores.map {
        case(score, oid) => SemanticData("term_to_term", distTermIds(termId), distTermIds(oid), score)
      }
    }

    sc.parallelize(singularVals) union termToConcept union docToConcept union termToTerm
  }

  /**
   * Returns
   *   - an RDD of rows of the document-term matrix,
   *   - a mapping of column indices to terms,
   *   - a mapping of row IDs to document titles.
   *   - idfs values
   */
  def preprocess() : (RDD[Vector], Map[Int, String], Map[Long, String], Map[String, Double]) = {
    val pages = Util.readToPageRdd(sc, input)

    val plainText = pages.filter(_ != null).flatMap(ParseWikipedia.wikiXmlToPlainText)

    val lemmatized = plainText.mapPartitions(iter => {
      val pipeline = ParseWikipedia.createNLPPipeline()
      iter.map{ case(title, contents) =>
        (title, ParseWikipedia.plainTextToLemmas(contents, stopWords, pipeline))}
    })

    val filtered = lemmatized.filter(_._2.size > 1)

    ParseWikipedia.documentTermMatrix(filtered, stopWords, numTermsCap, sc)
  }

  def topTermsInTopConcepts(numConcepts: Int, numTerms: Int): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map{case (score, id) => (termIds(id), score)}
    }
    topTerms
  }

  def topDocsInTopConcepts(numConcepts: Int, numDocs: Int): Seq[Seq[(String, Double)]] = {
    val u  = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
      topDocs += docWeights.top(numDocs).map{case (score, id) => (docIds(id), score)}
    }
    topDocs
  }
}

object LatentSemanticAnalyzer {
  def apply (sc: SparkContext, input: String, stopwords_fn: String, numConcepts: Int = 100,
  numTermsCap: Int = 50000) =
    new LatentSemanticAnalyzer(sc, input, stopwords_fn, numConcepts, numTermsCap)
  /**
   * Selects a row from a matrix.
   */
  def row(mat: BDenseMatrix[Double], index: Int): Seq[Double] = {
    (0 until mat.cols).map(c => mat(index, c))
  }

  /**
   * Selects a row from a matrix.
   */
  def row(mat: Matrix, index: Int): Seq[Double] = {
    val arr = mat.toArray
    (0 until mat.numCols).map(i => arr(index + i * mat.numRows))
  }

  /**
   * Selects a row from a distributed matrix.
   */
  def row(mat: RowMatrix, id: Long): Array[Double] = {
    mat.rows.zipWithUniqueId.map(_.swap).lookup(id).head.toArray
  }

  /**
   * Finds the product of a dense matrix and a diagonal matrix represented by a vector.
   * Breeze doesn't support efficient diagonal representations, so multiply manually.
   */
  def multiplyByDiagonalMatrix(mat: Matrix, diag: Vector): BDenseMatrix[Double] = {
    val sArr = diag.toArray
    new BDenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
      .mapPairs{case ((r, c), v) => v * sArr(c)}
  }

  /**
   * Finds the product of a distributed matrix and a diagonal matrix represented by a vector.
   */
  def multiplyByDiagonalMatrix(mat: RowMatrix, diag: Vector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map(vec => {
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      Vectors.dense(newArr)
    }))
  }

  /**
   * Returns a matrix where each row is divided by its length.
   */
  def rowsNormalized(mat: BDenseMatrix[Double]): BDenseMatrix[Double] = {
    val newMat = new BDenseMatrix[Double](mat.rows, mat.cols)
    for (r <- 0 until mat.rows) {
      val length = math.sqrt((0 until mat.cols).map(c => mat(r, c) * mat(r, c)).sum)
      (0 until mat.cols).map(c => newMat.update(r, c, mat(r, c) / length))
    }
    newMat
  }

  /**
   * Returns a distributed matrix where each row is divided by its length.
   */
  def rowsNormalized(mat: RowMatrix): RowMatrix = {
    new RowMatrix(mat.rows.map(vec => {
      val length = math.sqrt(vec.toArray.map(x => x * x).sum)
      Vectors.dense(vec.toArray.map(_ / length))
    }))
  }

  /**
   * Finds terms relevant to a term. Returns the term IDs and scores for the terms with the highest
   * relevance scores to the given term.
   */
  def topTermsForTerm(normalizedVS: BDenseMatrix[Double], termId: Int): Seq[(Double, Int)] = {
    // Look up the row in VS corresponding to the given term ID.
    val termRowVec = new BDenseVector[Double](row(normalizedVS, termId).toArray)

    // Compute scores against every term
    val termScores = (normalizedVS * termRowVec).toArray.zipWithIndex

    // Find the terms with the highest scores
    termScores.sortBy(-_._1).take(10)
  }

  /**
   * Finds docs relevant to a doc. Returns the doc IDs and scores for the docs with the highest
   * relevance scores to the given doc.
   */
  def topDocsForDoc(normalizedUS: RowMatrix, docId: Long): Seq[(Double, Long)] = {
    // Look up the row in US corresponding to the given doc ID.
    val docRowArr = row(normalizedUS, docId)
    val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)

    // Compute scores against every doc
    val docScores = normalizedUS.multiply(docRowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId

    // Docs can end up with NaN score if their row in U is all zeros.  Filter these out.
    allDocWeights.filter(!_._1.isNaN).top(10)
  }

  /**
   * Finds docs relevant to a term. Returns the doc IDs and scores for the docs with the highest
   * relevance scores to the given term.
   */
  def topDocsForTerm(US: RowMatrix, V: Matrix, termId: Int): Seq[(Double, Long)] = {
    val termRowArr = row(V, termId).toArray
    val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)

    // Compute scores against every doc
    val docScores = US.multiply(termRowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
    allDocWeights.top(10)
  }

  def termsToQueryVector(terms: Seq[String], idTerms: Map[String, Int], idfs: Map[String, Double])
    : BSparseVector[Double] = {
    val indices = terms.map(idTerms(_)).toArray
    val values = terms.map(idfs(_)).toArray
    new BSparseVector[Double](indices, values, idTerms.size)
  }

  def topDocsForTermQuery(US: RowMatrix, V: Matrix, query: BSparseVector[Double])
    : Seq[(Double, Long)] = {
    val breezeV = new BDenseMatrix[Double](V.numRows, V.numCols, V.toArray)
    val termRowArr = (breezeV.t * query).toArray

    val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)

    // Compute scores against every doc
    val docScores = US.multiply(termRowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
    allDocWeights.top(10)
  }

  def printTopTermsForTerm(normalizedVS: BDenseMatrix[Double],
      term: String, idTerms: Map[String, Int], termIds: Map[Int, String]) {
    printIdWeights(topTermsForTerm(normalizedVS, idTerms(term)), termIds)
  }

  def printTopDocsForDoc(normalizedUS: RowMatrix, doc: String, idDocs: Map[String, Long],
      docIds: Map[Long, String]) {
    printIdWeights(topDocsForDoc(normalizedUS, idDocs(doc)), docIds)
  }

  def printTopDocsForTerm(US: RowMatrix, V: Matrix, term: String, idTerms: Map[String, Int],
      docIds: Map[Long, String]) {
    printIdWeights(topDocsForTerm(US, V, idTerms(term)), docIds)
  }

  def printIdWeights[T](idWeights: Seq[(Double, T)], entityIds: Map[T, String]) {
    println(idWeights.map{case (score, id) => (entityIds(id), score)}.mkString(", "))
  }
}
