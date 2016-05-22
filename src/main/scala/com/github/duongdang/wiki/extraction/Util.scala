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
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.streaming.StreamXmlRecordReader
import org.apache.spark.SparkContext

object Util {
  def readDumpToPageRdd(sc: SparkContext, path: String) = {
    val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<page")
    jobConf.set("stream.recordreader.end", "</page>")
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf, path)

    // Load documents
    sc.hadoopRDD(jobConf,classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[org.apache.hadoop.io.Text],
      classOf[org.apache.hadoop.io.Text])
      .map(_._1.toString)
  }
}
