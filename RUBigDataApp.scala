// Example job 

package org.rubigdata

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.InputStreamReader;
import java.io.IOException;
import org.jsoup.Jsoup;
import nl.surfsara.warcutils.WarcInputFormat
import org.jwat.warc.{WarcConstants, WarcRecord}
import org.apache.hadoop.io.LongWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf


object RUBigDataApp {
  def main(args: Array[String]) {
   
    val conf = new SparkConf().setAppName("BigdataCustom")
    val sc = new SparkContext(conf)


    var rf = FileSystem.get(sc.hadoopConfiguration)
                             .listStatus(new Path(CCdir))
                             .flatMap(x =>
                                FileSystem.get(sc.hadoopConfiguration)
                                          .listStatus(x.getPath().suffix("/warc/"))
                                          .map(y => y.getPath))
                             .mkString(",")

    val warcfile = sc.newAPIHadoopFile(
                rf,
                classOf[WarcInputFormat],               // InputFormat
                classOf[LongWritable],                  // Key  
                classOf[WarcRecord]                     // Value
        )
	   	  
	  
    val warcc = warcfile.
      filter{ _._2.header.warcTypeIdx == 2 }.
      filter{ _._2.getHttpHeader.contentType != null }.
      filter{ _._2.getHttpHeader().contentType.startsWith("text/html") }.
      filter{ _._2.header.contentLength.toInt > 0 }.
      map{wr => ( wr._2.header.warcTargetUriStr, getContent(wr._2) )}		  
	
 // loaded in all warcfiles
	  
	  def getContent(record: WarcRecord):String = {
	  val cLen = record.header.contentLength.toInt
	  //val cStream = record.getPayload.getInputStreamComplete()
	  val cStream = record.getPayload.getInputStream()
	  val content = new java.io.ByteArrayOutputStream();

	  val buf = new Array[Byte](cLen)
	  
  	  var nRead = cStream.read(buf)
	  while (nRead != -1) {
		content.write(buf, 0, nRead)
		nRead = cStream.read(buf)
	  }

	  cStream.close()
	  
	  content.toString("UTF-8");
	}
  	
	

	def HTML2Txt(content: String) = {
	  try {
		     val htmltree = Jsoup.parse(content).text().replaceAll("[\\r\\n]+", " ")
		    // htmltree.select(".article_body").first.text()
	  }
	  catch {
		case e: Exception => ""
	  }
	}

	  
	  //////////////////////////////////////
	  
	  val contents = warcc.map{page => (page._1, page._2)}.filter(_._2 != "").map{ pp => pp._2}

//	contents.take(1)

	//val body = articles.map{ tt => (tt._1, tt._2)}.filter(_._2 != "").map{tt => tt._2}
	//contents.count





 	val listwords = contents.flatMap{(article => article.split(" "))}
                             .filter (_ != "")
                             .map(word =>(word.toLowerCase,1))
                            


	val wc = listwords.reduceByKey(_ + _) 
	val top20 = wc.takeOrdered(20)(Ordering[Int].reverse.on(x=>x._2)).take(10)


	val lijsttrekkers = "rutte klaver pechtold".split(" ")


	val rutte = wc.filter(wr => lijsttrekkers.contains(wr._1))

	rutte.take(10).foreach(tuple=>println(tuple))
	// val numAs = data.filter(line => line.contains("a")).count()
    	// val numBs = data.filter(line => line.contains("b")).count()
    	// println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))	  

	}
}
