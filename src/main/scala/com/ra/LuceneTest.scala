package com.ra

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document.{Document, Field, FieldType}
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts
import org.apache.lucene.facet._
import org.apache.lucene.facet.sortedset.{DefaultSortedSetDocValuesReaderState, SortedSetDocValuesFacetCounts, SortedSetDocValuesFacetField}
import org.apache.lucene.facet.taxonomy.directory.{DirectoryTaxonomyReader, DirectoryTaxonomyWriter}
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{DirectoryReader, IndexOptions, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery}
import org.apache.lucene.store.{Directory, RAMDirectory}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

//import scala.io.Source

object LuceneTest extends App {

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()

  val TMP_DIR = System.getProperty("java.io.tmpdir")


  def createIndex(): Future[Seq[Document]] = {


    val textIndexedType = new FieldType()


    textIndexedType.setStored(true)
    textIndexedType.setIndexOptions(IndexOptions.DOCS_AND_FREQS)
    textIndexedType.setTokenized(true)


    val d = new File("/home/ra/wtest/help/")


    Source(d.listFiles().filter(_.isFile).toList)
      .map { x =>
        val document = new Document()

        val titleStr = x.getName
        val bodyStr = scala.io.Source.fromFile(x.toString).getLines().fold("")(_ ++ _)

        val title = new Field("title", titleStr, textIndexedType)
        val body = new Field("body", bodyStr, textIndexedType)
        document.add(title)
        document.add(body)
        document
      }
      .runFold(Seq.empty[Document])(_ :+ _)

//    stream.onComplete { x =>
//      x.get.foreach { doc =>
//        println(doc.getField("title"))
//        println(doc.getField("body"))
//      }
//    }
  }

// def index(indexDir: File, dataDir: File, suffix: String): Unit = {
//
//   val indexWriter = new IndexWriter(
//       FSDirectory.open(indexDir),
//   new SimpleAnalyzer(),
//   true,
//   IndexWriter.)
//
//   indexWriter.setUseCompoundFile(false)
//
// }

}

//object Test extends App {
//
//  def runTest = {
//
//    import org.apache.lucene.analysis.standard.StandardAnalyzer
//    import org.apache.lucene.document.TextField
//    import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
//    import org.apache.lucene.search.IndexSearcher
//    import org.apache.lucene.store.FSDirectory
//    import org.apache.lucene.util.Version
//
//    val path = new File("/home/ra/lucene/index")
//    val analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT)
//    val directory = FSDirectory.open(path)
//    val config = new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer)
//    val iwriter = new IndexWriter(directory, config)
//    val doc = new Document
//    val text = "This is the text to be indexed.2"
//    doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
//    iwriter.addDocument(doc)
//    iwriter.commit()
//
//
//
//    val ireader = DirectoryReader.open(directory)
//    val isearcher = new IndexSearcher(ireader)
//    val parser = new QueryParser(Version.LUCENE_CURRENT, "fieldname", analyzer)
//    val query = parser.parse("text")
//    val hits = isearcher.search(query, null, 1000).scoreDocs
//    //  assertEquals(1, hits.length)
//
//    if (hits.length > 0) println("ok") else println("error")
//
//    val hitDoc = isearcher.doc(hits(1).doc)
//    hitDoc.getFields("fieldname").foreach(x => println(x.toString))
//
//
//    ireader.close()
//    directory.close()
//
//  }
//  runTest
//}


class SimpleFacetExample(indexDir: Directory,
                         taxonomyDir: Directory,
                         facetsConfig: FacetsConfig = new FacetsConfig()) {


  def index(docs: Seq[Document]): Unit = {
    val indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(
      new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE_OR_APPEND))

    // Writes facet ords to a separate directory from the main index
    val taxoWriter = new DirectoryTaxonomyWriter(taxonomyDir)

    docs.foreach( doc =>
      indexWriter.addDocument(facetsConfig.build(taxoWriter, doc))
    )

    indexWriter.close()
    taxoWriter.close()
  }

  def index(): Unit = {

    val docs = new ArrayBuffer[Document]()

    val doc = new Document()
    doc.add(new FacetField("Author", "Bob"))
    doc.add(new FacetField("Publish Date", "2010"))
    docs += doc

    val doc2 = new Document()
    doc2.add(new FacetField("Author", "Lisa"))
    doc2.add(new FacetField("Publish Date", "2010"))
    docs += doc2

    val doc3 = new Document()
    doc3.add(new FacetField("Author", "Lisa"))
    doc3.add(new FacetField("Publish Date", "2012"))
    docs += doc3

    val doc4 = new Document()
    doc4.add(new FacetField("Author", "Susan"))
    doc4.add(new FacetField("Publish Date", "2012"))
    docs += doc4

    val doc5 = new Document()
    doc5.add(new FacetField("Author", "Frank"))
    doc5.add(new FacetField("Publish Date", "1999"))
    docs += doc5

    val doc6 = new Document()
    doc6.add(new FacetField("Author", "Anastasios"))
    doc6.add(new FacetField("Publish Date", "1984"))
    docs += doc6

    index(docs)
  }


  def facetsOnly(indexDir: RAMDirectory, taxoDir: RAMDirectory): List[FacetResult] = {
    val indexReader = DirectoryReader.open(indexDir)
    val searcher = new IndexSearcher(indexReader)
    val taxoReader = new DirectoryTaxonomyReader(taxoDir)

    val fc = new FacetsCollector()

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index) normally
    // you'd use a "normal" query:
    searcher.search(new MatchAllDocsQuery(), fc)

    // Retrieve results
    val results = new ArrayBuffer[FacetResult]

    // Count both "Publish Date" and "Author" dimensions
    val facets = new FastTaxonomyFacetCounts(taxoReader, facetsConfig, fc)

    results += facets.getTopChildren(10, "Author")
    results += facets.getTopChildren(10, "Publish Date")

    indexReader.close()
    taxoReader.close()

    results.toList
  }
}

object SimpleFacetExample {

  private val indexDir = new RAMDirectory()
  private val taxoDir = new RAMDirectory()

  def main(args: Array[String]): Unit = {
    val example = new SimpleFacetExample(indexDir, taxoDir)

    // Index sample docs
    example.index()

    example.index()


    // Printout the facets
    val facets = example.facetsOnly(indexDir, taxoDir)
    facets.foreach(FacetUtils.printFacetResult(_))
  }
}

object FacetUtils {

  def facetValuestoString(facetValues: Array[LabelAndValue]): String = {
    facetValues.map(x => s"  ${x.label} (${x.value})").mkString("\n")
  }

  def printFacetResult(facetResult: FacetResult): Unit = {
    // scalastyle:off println
    println("********************************")
    println("Facet:")
    println("********************************")
    println(s"* Facet name: ${facetResult.dim}")
    println(s"* Sum of facets counts : ${facetResult.value}")
    println(s"* # of facets : ${facetResult.childCount}")
    // println(s"* Path: ${facetResult.path.mkString("/")}")
    println(s"* Value (count): \n${facetValuestoString(facetResult.labelValues)}")
    println("********************************")
    // scalastyle:on println
  }


}


class SimpleSortedSetFacetsExample(indexDir: Directory,
                                   facetsConfig: FacetsConfig = new FacetsConfig()) {


  /**
    * Index documents
    * @param docs
    */
  def index(docs: Seq[Document]): Unit = {
    val wsAnalyzer = new WhitespaceAnalyzer()
    val indexWriterConfig = new IndexWriterConfig(wsAnalyzer).setOpenMode(OpenMode.CREATE_OR_APPEND)
    val indexWriter = new IndexWriter(indexDir, indexWriterConfig)

    docs.foreach( doc => indexWriter.addDocument(facetsConfig.build(doc)))

    indexWriter.close()
  }

  def index(): Unit = {

    val docs = new ArrayBuffer[Document]()

    val doc = new Document()
    doc.add(new SortedSetDocValuesFacetField("Author", "Bob"))
    doc.add(new SortedSetDocValuesFacetField("Publish Date", "2010"))
    docs += doc

    val doc2 = new Document()
    doc2.add(new SortedSetDocValuesFacetField("Author", "Lisa"))
    doc2.add(new SortedSetDocValuesFacetField("Publish Date", "2010"))
    docs += doc2

    val doc3 = new Document()
    doc3.add(new SortedSetDocValuesFacetField("Author", "Lisa"))
    doc3.add(new SortedSetDocValuesFacetField("Publish Date", "2012"))
    docs += doc3

    val doc4 = new Document()
    doc4.add(new SortedSetDocValuesFacetField("Author", "Susan"))
    doc4.add(new SortedSetDocValuesFacetField("Publish Date", "2012"))
    docs += doc4

    val doc5 = new Document()
    doc5.add(new SortedSetDocValuesFacetField("Author", "Frank"))
    doc5.add(new SortedSetDocValuesFacetField("Publish Date", "1999"))
    docs += doc5

    val doc6 = new Document()
    doc6.add(new SortedSetDocValuesFacetField("Author", "Anastasios"))
    doc6.add(new SortedSetDocValuesFacetField("Publish Date", "1984"))
    docs += doc6

    index(docs)
  }


  def facetsOnly(indexDir: Directory): List[FacetResult] = {
    val indexReader = DirectoryReader.open(indexDir)
    val searcher = new IndexSearcher(indexReader)

    val state = new DefaultSortedSetDocValuesReaderState(indexReader)

    // Aggregates the facet counts
    val fc = new FacetsCollector()

    FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, fc)

    // Retrieve results
    val results = new ArrayBuffer[FacetResult]

    // Count both "Publish Date" and "Author" dimensions
    // Retrieve results
    val facets: Facets = new SortedSetDocValuesFacetCounts(state, fc)

    results += facets.getTopChildren(10, "Author")
    results += facets.getTopChildren(10, "Publish Date")

    indexReader.close()

    results.toList
  }
}

object SimpleSortedSetFacetsExample {

  private val indexDir = new RAMDirectory()

  def main(args: Array[String]): Unit = {
    val example = new SimpleSortedSetFacetsExample(indexDir)

    // Index sample docs
    example.index()

    // Printout the facets
    val facets = example.facetsOnly(indexDir)
    facets.foreach(FacetUtils.printFacetResult(_))
  }
}


