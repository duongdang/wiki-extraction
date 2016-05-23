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

import scala.xml.XML
import org.apache.spark.SparkContext
import org.apache.hadoop.io.{LongWritable, Text}

object Util {
  def readDumpToPageRdd(sc: SparkContext, path: String) = {
    sc.newAPIHadoopFile(
      path,
      classOf[WikiInputFormat],
      classOf[LongWritable],
      classOf[Text])
      .map(_._2.toString)
  }
}
