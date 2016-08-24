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
package com.github.duongdang.wikidata
import org.json4s._
import org.json4s.native.JsonMethods._


import java.io.Serializable

case class WikidataLabel (
  entityId: String,
  entityType: String,
  language: String,
  label: String,
  description: String
) extends Serializable

object WikidataLabel {
  case class Text(language: String, value: String)

  def fromText(input: String) = {
    val json = parse(input)
    implicit val formats = DefaultFormats
    val entityId = (json \ "id").extract[String]
    val entityType = (json \ "type").extract[String]

    val labels = (json \ "labels").extract[Map[String, Text]]
    val descriptions = (json \ "descriptions").extract[Map[String, Text]]

    labels.map {
      case (lang, text) => WikidataLabel(entityId, entityType, lang, text.value,
        descriptions.getOrElse(lang, Text("","")).value)
    }
  }
}


case class WikidataSiteLink (
  entityId: String,
  entityType: String,
  site: String,
  title: String,
  url: String
) extends Serializable

object WikidataSiteLink {
  case class Link(site: String, title: String, url: String)

  def fromText(input: String) = {
    val json = parse(input)
    implicit val formats = DefaultFormats
    val entityId = (json \ "id").extract[String]
    val entityType = (json \ "type").extract[String]

    val siteLinks = (json \ "sitelinks").extract[Map[String, Link]]

    siteLinks.map {
      case (site, link) => WikidataSiteLink(entityId, entityType, site, link.title, link.url)
    }
  }
}

case class WikidataClaim (
  entityId: String,
  propertyId: String,
  dataType: String,
  valueType: String,
  valueValue: String
) extends Serializable

object WikidataClaim {
  def fromText(input: String) = {
    val json = parse(input)
    implicit val formats = DefaultFormats
    val entityId = (json \ "id").extract[String]
    val entityType = (json \ "type").extract[String]

    val buf = scala.collection.mutable.ArrayBuffer.empty[WikidataClaim]
    for {
      JArray(objList) <- (json \ "claims" \\ "mainsnak")
      JObject(objs) <- objList
    }{
      objs.map { case (k, snak) =>
        val propertyId = (snak \ "property").extract[String]
        val dataType = (snak \ "datatype").extract[String]
        val valueType = (snak \ "datavalue.type").extract[String]
        val valueValue: String = valueType match {
          case "wikibase-entityid" => (snak \ "datavalue.value.id").extract[String]
          case "string" => (snak \ "datavalue.value").extract[String]
          case default => compact(render(snak \ "datavalue.value"))
        }
        buf += WikidataClaim(entityId, propertyId, dataType, valueType, valueValue)
      }
    }
    buf.toArray
  }
}
