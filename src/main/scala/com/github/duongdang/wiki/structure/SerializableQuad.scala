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
package com.github.duongdang.wiki.structure

import org.dbpedia.extraction.destinations.Quad
import java.io.Serializable

case class SerializableQuad (
  language: String,
  dataset: String,
  subject: String,
  predicate: String,
  value: String,
  context: String,
  datatype: String
) extends Ordered[SerializableQuad] with Serializable {
  import scala.math.Ordered.orderingToOrdered
  def compare(that: SerializableQuad): Int =
    (this.language, this.dataset, this.subject,
      this.predicate, this.value, this.context, this.datatype) compare
    (that.language, that.dataset, that.subject,
      that.predicate, that.value, that.context, that.datatype)
}

object SerializableQuad {
  def apply(quad: Quad) : SerializableQuad =
    SerializableQuad(quad.language, quad.dataset, quad.subject,
      quad.predicate, quad.value, quad.context, quad.datatype)
}
