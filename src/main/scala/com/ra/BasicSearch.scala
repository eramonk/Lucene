package com.ra

import java.nio.file.Paths
import java.util

import com.ra.Print._
import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.lucene.facet.taxonomy.{FacetLabel, FastTaxonomyFacetCounts}
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader
import org.apache.lucene.facet.{FacetResult, FacetsCollector, FacetsConfig}
import org.apache.lucene.index.{DirectoryReader, Term}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search._
import org.apache.lucene.store.FSDirectory

class BasicSearch(path: String) {

  val reader = DirectoryReader.open(FSDirectory.open(Paths.get(path)))

  val DEFAULT_LIMIT = 10

  def searchIndexWithTermQuery(toSearch: String, searchField: String, limit: Int) = {

    val indexSearcher = new IndexSearcher(reader)
    val term = new Term(searchField, toSearch)
    val query = new TermQuery(term)
    val search = indexSearcher.search(query, limit)
    val hits: Array[ScoreDoc] = search.scoreDocs
    showHits(hits)

  }

  def searchInBody(toSearch: String, searchField: String, limit: Int) = {

    val indexSearcher = new IndexSearcher(reader)
    val queryParser = new QueryParser(searchField, new RussianAnalyzer())
    val query: Query = queryParser.parse(toSearch)
    val search = indexSearcher.search(query, limit)
    val hits = search.scoreDocs
    showHits(hits)
  }

  def showHits(hits: Array[ScoreDoc]) = {

    if (hits.length == 0) {
      println("\n\tНичего не найдено")
    }

    println("\n\tРезультаты поиска:")

    hits.foreach { x =>
      val title = reader.document(x.doc).get("title")
      val body = reader.document(x.doc).get("body")
      val category = reader.document(x.doc).get("category")

      println(x.doc, category, title, body)
    }

  }


  def facetsWithSearch() = {
    val config = new FacetsConfig()
    val taxoDir = Paths.get("/home/ra/lucene/taxonomy/")

    val queryParser = new QueryParser("body", new RussianAnalyzer())
    val query: Query = queryParser.parse("документ")

    val indexReader = DirectoryReader.open(FSDirectory.open(Paths.get("/home/ra/lucene/newindex/")))
    val searcher = new IndexSearcher(indexReader)
    val taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(taxoDir))

    val fc: FacetsCollector = new FacetsCollector()
    searcher.search(query, fc)
    //    FacetsCollector.search(searcher, new MatchAllDocsQuery("документ"), 10, fc)
    val results = new util.ArrayList[FacetResult]
    val results2 = new util.ArrayList[FacetResult]

    val facets = new FastTaxonomyFacetCounts(taxoReader, config, fc)

    results.add(facets.getTopChildren(10, "category"))
    results2.add(facets.getTopChildren(10, "Пользовательский интерфейс"))

    //    results.add(facets.getTopChildren(9, "Виджет"))
    //    results.add(facets.getTopChildren(10, "Publish Date"))

    //    val hits = facets.getTopChildren(10, "Виджет")


    indexReader.close()
    taxoReader.close()
    (results, results2)

  }

  def searchInBodyWithFacet(toSearch: String, limit: Int, searchField: String = "body", facetLabel: String = "category"): Unit = {

    val indexSearcher = new IndexSearcher(reader)
    val queryParser = new QueryParser(searchField, new RussianAnalyzer())
    val query: Query = queryParser.parse(toSearch)
    val search = indexSearcher.search(query, limit)
    val hits = search.scoreDocs
    val config = new FacetsConfig()
    val taxoDir = Paths.get("/home/ra/lucene/taxonomy/")
    val taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(taxoDir))
    //    showHits(hits)
    val hitsId = hits.map { x =>
            val fac = taxoReader.getPath(x.doc).components(0)
            taxoReader.getPath(x.doc).components(1)
            taxoReader.getOrdinal(new FacetLabel(fac))
      responseStr(reader.document(x.doc).getField("body").stringValue(), toSearch)
            reader.document(x.doc).getField("body").stringValue().print
//            reader.document(x.doc).getField("category").print
    }
    //    println(reader.document(n.childCount))


    //    val queryParser = new QueryParser("body", new RussianAnalyzer())
    //    val query: Query = queryParser.parse("документ")

    val indexReader = DirectoryReader.open(FSDirectory.open(Paths.get("/home/ra/lucene/newindex/")))
    val searcher = new IndexSearcher(indexReader)
    //    val taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(taxoDir))

    val fc: FacetsCollector = new FacetsCollector()
    searcher.search(query, fc)

    val facets = new FastTaxonomyFacetCounts(taxoReader, config, fc)


    val res = facets.getTopChildren(10, facetLabel)
    //    val res1 = facets.getTopChildren(10, "category")

    res.labelValues.foreach(x => println(x.label, x.value))

    println(res.dim, res.childCount, res.labelValues, res.path, res.value)


    //    facets.getTopChildren(10, "Пользовательский интерфейс")


  }

  def responseStr(body: String, toSearch: String) = {
    val strOfSearch = toSearch.split(" ")

    strOfSearch.map { x =>
      val index = body.indexOf(x)
      if (body.contains(x)) {
        body.toList.slice(index - 75, index + 75).mkString
      }
    }
  }
}

object TestSearch extends App {
  val pathToIndex = "/home/ra/lucene/newindex/"
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

  new BasicSearch(pathToIndex).searchInBodyWithFacet("элементов документа", 100)

}
