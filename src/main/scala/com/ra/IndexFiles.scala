package com.ra

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util
import java.util.Date
import javax.xml.parsers.DocumentBuilder

import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.document._
import org.apache.lucene.facet.taxonomy.FacetLabel
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index._
import org.apache.lucene.store.FSDirectory
import org.jsoup.Jsoup
import com.ra.Print._
import org.apache.lucene.facet.{FacetField, FacetsConfig}
class IndexFiles {

  var count = 0
  val textIndexedType = new FieldType()

  textIndexedType.setStored(true)
  textIndexedType.setIndexOptions(IndexOptions.DOCS_AND_FREQS)
  textIndexedType.setTokenized(true)
  textIndexedType.setStoreTermVectors(true)

  def indexFiles(indexPath: String, docsPath: String, taxoPath: String, create: Boolean): Unit = {

    val docDir = Paths.get(docsPath)
    val taxoDir = Paths.get(taxoPath)
    if (!Files.isReadable(docDir)) throw new Error("Bad docDir")

    val start = new Date()

    val dir = FSDirectory.open(Paths.get(indexPath))
    val taxo = FSDirectory.open(taxoDir)

    val taxoWriter: DirectoryTaxonomyWriter = new DirectoryTaxonomyWriter(taxo, OpenMode.CREATE)

    val analyzer = new RussianAnalyzer()
    val iwc = new IndexWriterConfig(analyzer)

    if (create) {
      iwc.setOpenMode(OpenMode.CREATE)
    } else iwc.setOpenMode(OpenMode.CREATE_OR_APPEND)

    val writer = new IndexWriter(dir, iwc)

    indexDocs(writer, taxoWriter, docDir)

    writer.close()
    taxoWriter.close()
    val end = new Date()
    println(end.getTime - start.getTime + " total milliseconds", count)




  }

  def indexDocs(writer: IndexWriter, taxoWriter: DirectoryTaxonomyWriter, path: Path) = {
    if (Files.isDirectory(path)) {
      Files.walkFileTree(path, new SimpleFileVisitor[Path]() {
        println("!!!!!!!!!!!!!!", path.getFileName.toString)
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {

          val category =  file.getName(5).toString

          indexDoc(writer, taxoWriter, file, attrs.lastModifiedTime().toMillis, category)
          FileVisitResult.CONTINUE
        }
      })
    } else indexDoc(writer, taxoWriter, path, Files.getLastModifiedTime(path).toMillis, path.getFileName.toString)
  }

  def indexDoc(writer: IndexWriter, taxoWriter: DirectoryTaxonomyWriter, file: Path, lastModified: Long, category: String): Unit = {

    if (file.toString.endsWith(".html")) {

      val config = new FacetsConfig()
      config.setIndexFieldName("category", "my_cat")
//      config.setHierarchical("category", true)



      val fis = Files.newInputStream(file)
      val doc = new Document()
      val pathField = new StringField("path", file.getFileName.toString, Field.Store.YES)

//      val categories = new util.ArrayList[FacetLabel]
//      categories.add(new FacetLabel(category))
//      categories.add(new FacetLabel("year", "2017"))
//
//      val categoryDocBuilder = new DocumentBuilder(taxoWriter)


//      val titleStr = file.toString

      val bodyStr = scala.io.Source.fromFile(file.toString).getLines().fold("")(_ ++ _)
      val bodyWithOutHtml = Jsoup.parse(bodyStr).text()
//      val titleStr = Jsoup.parse(bodyStr).body().getElementsByClass("titlepage").text().print
      val titleStr = Jsoup.parse(bodyStr).select("div.titlepage").eq(0).text()

//      val analyzer = new RussianAnalyzer()
//
//      val stream = analyzer.tokenStream(null, new StringReader(bodyWithOutHtml))
//      val cattr = stream.addAttribute(classOf[CharTermAttribute])
//      stream.reset()
//
//      while(stream.incrementToken()) {
//        print(cattr.toString + " ")
//      }
//
//      stream.end()
//      stream.close()


      doc.add(pathField)
//      doc.add(new LongPoint("modified", lastModified))
//      doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8))))
//      doc.add(new Field("title", titleStr, textIndexedType))
      doc.add(new Field("body", bodyWithOutHtml, textIndexedType))
      doc.add(new Field("title", titleStr, textIndexedType))
//      doc.add(new FacetField("root", "help"))
      println("000000000000000",category)
      println("11111111111111", file)
      doc.add(new Field(file.getName(5).toString.print, file.toString.print, textIndexedType))

//      doc.add(new Field("category", category, textIndexedType))
//      doc.add(new FacetField("category", file.getName(0).toString, file.getName(1).toString, file.getName(2).toString, file.getName(3).toString, file.getName(4).toString))
      doc.add(new FacetField("category", file.getName(5).toString.split("_").head.print))

//      doc.add(new FacetField("category", category))

      if (writer.getConfig.getOpenMode == OpenMode.CREATE) {
        count += 1
        println("adding " + file)
        writer.addDocument(config.build(taxoWriter, doc))
        writer.commit()
//        taxoWriter.close()
      }
      else {
        println("updating " + file)
        writer.updateDocument(new Term("path", file.toString), config.build(taxoWriter, doc))
        writer.commit()
//        taxoWriter.close()
      }
//      taxoWriter.close()
    }
//    taxoWriter.close()
//    writer.close()
  }


}

object Test2 extends App {
  val create: Boolean = true
  new IndexFiles().indexFiles("/home/ra/test/newindex1/", "/home/ra/test/help/", "/home/ra/test/taxonomy1/", create)
}
