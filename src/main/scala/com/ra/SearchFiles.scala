package com.ra

import java.io.{BufferedReader, File}
import java.nio.file.Paths

import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, Query}
import org.apache.lucene.store.FSDirectory
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Date

import org.apache.lucene.queryparser.classic.QueryParser

class SearchFiles {

  def searchFiles(index: String,
                  field: String,
                  queries: String = null,
                  repeat: Int = 0,
                  raw: Boolean = false,
                  queryString: String = null,
                  hitsPerPage: Int = 10): Unit = {

    val reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)))
    val searcher = new IndexSearcher(reader)
    val analyzer = new RussianAnalyzer()

    var in: BufferedReader = null


    if (queries != null) {
      in = Files.newBufferedReader(Paths.get(queries), StandardCharsets.UTF_8)
    }
    else {
      in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))
    }

    val parser = new QueryParser(field, analyzer)

    while (true) {
      if (queries == null && queryString == null) {
        System.out.println("Enter query: ")
      }

      var line: String = if (queryString != null) queryString.trim() else in.readLine().trim()

      val query = parser.parse(line)

      println("Searching for: " + query.toString(field))

      if (repeat > 0) {
        val start = new Date()
        (0 to repeat).map { _ => searcher.search(query, 100) }
        val end = new Date()
        println("Time: " + (end.getTime - start.getTime + "ms"))
      }

      doPagingSearch(in, searcher, query, hitsPerPage, raw, queries == null && queryString == null)
    }
    reader.close()
  }

  def doPagingSearch(in: BufferedReader, searcher: IndexSearcher, query: Query,
                     hitsPerPage: Int, raw: Boolean, interactive: Boolean) = {

    val results = searcher.search(query, 5 * hitsPerPage)
    var hits = results.scoreDocs
    val numTotalHits = Math.toIntExact(results.totalHits)
    println(numTotalHits + " total matching documents")

    val start = 0
    var end = Math.min(numTotalHits, hitsPerPage)

    while(true) {
      if(end > hits.length) {
        println("Only results 1 - " + hits.length +" of " + numTotalHits + " total matching documents collected.")
        println("Collect more (y/n) ?")
        val line = in.readLine()
        hits = searcher.search(query, numTotalHits).scoreDocs
      }

      end = Math.min(hits.length, start + hitsPerPage)

      hits.foreach{x =>
        val doc = searcher.doc(x.doc)
        val path = doc.get("path")
        val title = doc.get("title")
        println(path)
        println("   Title: " + doc.get("title"))
      }




    }
  }


}

object TestSearch2 extends App {
  new SearchFiles().searchFiles("/home/ra/lucene/newindex/", "поиск")
}
