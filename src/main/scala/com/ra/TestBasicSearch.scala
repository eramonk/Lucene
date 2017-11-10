package com.ra

import java.nio.file.Paths

import com.ra.Print._
import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts
import org.apache.lucene.facet.{FacetResult, FacetsCollector, FacetsConfig}
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.highlight.{Highlighter, QueryScorer, SimpleHTMLFormatter}
import org.apache.lucene.search.{IndexSearcher, Query}
import org.apache.lucene.store.FSDirectory
import play.api.libs.json.{JsValue, Json}

class TestBasicSearch(path: String) {

  case class Hit(url: String, title: String, highlighter: String, boost: Int)

  object Hit {
    implicit val hitWrites = Json.writes[Hit]
  }

  case class Hits(category: String, hits: Array[Hit], total: Int)

  object Hits {
    implicit val hitsWrites = Json.writes[Hits]
  }

  case class JsonResponse(hits: Array[Hits], total: Int)

  object JsonResponse {
    implicit val jsonResponse = Json.writes[JsonResponse]
  }

  def searchInBodyWithFacet(toSearch: String,
                            limit: Int,
                            searchField: String = "body") = {
    val reader = DirectoryReader.open(FSDirectory.open(Paths.get(path)))
    val indexSearcher = new IndexSearcher(reader)
    val queryParser = new QueryParser(searchField, new RussianAnalyzer())
    val query: Query = queryParser.parse(toSearch)
    val search = indexSearcher.search(query, limit)
    val hits = search.scoreDocs
    val scorer = new QueryScorer(query)

//        val highlighter = new Highlighter()
    val config = new FacetsConfig()
    config.setIndexFieldName("category", "my_cat")
//    config.setHierarchical("category", true)


    val taxoDir = Paths.get("/home/ra/test/taxonomy1/")
    val taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(taxoDir))

    val fc = new FacetsCollector()
    FacetsCollector.search(indexSearcher, query, limit, fc)
    val facetsFolder = new FastTaxonomyFacetCounts(
      "my_cat", taxoReader, config, fc)



      val result1 = facetsFolder.getTopChildren(100, "category").labelValues
    result1.foreach(x => println(x.label, x.value))
    println(result1)
//    println(facetsFolder.getTopChildren(10, "category").value)
//    println(facetsFolder.getTopChildren(10, "Пользовательский интерфейс").childCount)
//    println(facetsFolder.getTopChildren(10, "Пользовательский интерфейс").dim)
//    println(facetsFolder.getTopChildren(10, "Пользовательский интерфейс").labelValues)
//    println(facetsFolder.getTopChildren(10, "Пользовательский интерфейс").toString)

    val result: Array[(Map[String, Seq[String]], Int)] = hits.map { x =>
      val body = indexSearcher.doc(x.doc).get("body")
      val highlighter =
        new Highlighter(new SimpleHTMLFormatter("<mark>", "</mark>"), scorer)
          .getBestFragment(new RussianAnalyzer(), toSearch, body)
      val hitsUrl
        : String = path + reader.document(x.doc).getField("path").stringValue()
      val title = reader.document(x.doc).getField("title").stringValue()
//      val category = reader.document(x.doc).getField("category").stringValue()
//      val facet = taxoReader


      Map("test" -> Seq(hitsUrl, title, highlighter))

    }.zipWithIndex

    hitsToJson(result)

  }

  def hitsToJson(
      categoryAndHits: Array[(Map[String, Seq[String]], Int)]): JsValue = {

    val allCategoryName = categoryAndHits.map { x =>
      x._1.keys.head
    }.distinct

    val allHits = allCategoryName.map { x =>
      val filteredHits = categoryAndHits.filter { y =>
        y._1.keys.head == x
      }
      val hits: Array[Hit] = filteredHits.map { z =>
        Hit(z._1(x).head, z._1(x)(1), z._1(x)(2), z._2)
      }
      Hits(x, hits, filteredHits.length)
    }
    Json.toJson(JsonResponse(allHits, categoryAndHits.length))
  }
}

object TestSearch22 extends App {
  val pathToIndex = "/home/ra/test/newindex1/"
  //  new BasicSearch(reader).searchIndexWithTermQuery("режим", "body", 10)
  //  new BasicSearch(reader).searchInBody("Все сведения о конкретном", "body", 10)
  //  new BasicSearch(reader).searchInBody("Все сведения о конкретном", "title", 10)

  //  val res = new BasicSearch(pathToIndex).facetsWithSearch()
  //  val n: FacetResult = res._1.get(0)
  //  val n2: FacetResult = res._2.get(0)

  //  println(n.labelValues.foreach{println(_)})
  //  println("!!!!!!", n.labelValues(1), n.dim)
  //  println(n.labelValues(2))
  //  println(n.labelValues(3))
  //  println(reader.document(n.childCount))
  //  println(reader.document(n2.childCount))
  //  println(n2)
  //  println(n2)

  val res = new TestBasicSearch(pathToIndex)
    .searchInBodyWithFacet("пользователь", 100)
    .print

  //  new TestBasicSearch(pathToIndex).hitsToJson(res)
}

object Test extends App {


}