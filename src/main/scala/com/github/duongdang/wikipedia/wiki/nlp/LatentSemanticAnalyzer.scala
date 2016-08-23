/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.github.duongdang.wikipedia.nlp

import breeze.linalg.{DenseMatrix => BDenseMatrix, DenseVector => BDenseVector,
SparseVector => BSparseVector}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix


import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import java.io.Serializable

case class SemanticData(
  data_type: String,
  ref: String,
  key: String,
  score: Double
) extends Serializable

class LatentSemanticAnalyzer(@transient sc: SparkContext, input: String, stopwordsFn: String,
  numConcepts: Int = 1000,
  numTermsCap: Int = 50000) extends Serializable {

  @transient val (termDocMatrix, termIds, docIds, idfs) = WikiParser.parse(sc, input, stopwordsFn, numTermsCap)
  termDocMatrix.cache()
  @transient val mat = new RowMatrix(termDocMatrix)
  @transient val svd = mat.computeSVD(numConcepts, computeU=true)

  val vArray = sc.broadcast(svd.V.toArray).value
  val sArray = sc.broadcast(svd.s.toArray).value
  val vsArray = {
    val VS = LatentSemanticAnalyzer.multiplyByDiagonalMatrix(svd.V, svd.s)
    val normalizedVS = LatentSemanticAnalyzer.rowsNormalized(VS)
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
        case(score, oid) => SemanticData("term_to_term",
          distTermIds(termId), distTermIds(oid), score)
      }
    }

    sc.parallelize(singularVals) union termToConcept union docToConcept union termToTerm
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
  def apply (sc: SparkContext, input: String, stopwordsFn: String, numConcepts: Int = 100,
  numTermsCap: Int = 50000) =
    new LatentSemanticAnalyzer(sc, input, stopwordsFn, numConcepts, numTermsCap)
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

}
