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
reset( lastChanges= _.
      set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" ).
      set( "spark.kryo.classesToRegister", 
          "org.apache.hadoop.io.LongWritable," +
          "org.jwat.warc.WarcRecord," +
          "org.jwat.warc.WarcHeader" )
      )

object RUBigDataApp {
  def main(args: Array[String]) {
   
    val conf = new SparkConf().setAppName("RUBigDataApp")
    val sc = new SparkContext(conf)

    val warcfile = "data/public/common-crawl/crawl-data/CC-MAIN-2016-07/segments/1454701145578.23/warc/CC-MAIN-20160205193905-00185-ip-10-236-182-209.ec2.internal.warc.gz"
	
    val warcf = sc.newAPIHadoopFile(
              warcfile,
              classOf[WarcInputFormat],               // InputFormat
              classOf[LongWritable],                  // Key
              classOf[WarcRecord]                     // Value
    )   
		
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
		  
		     val htmltree = Jsoup.parse(content)//.text().replaceAll("[\\r\\n]+", " ")
		     htmltree.select(".article_body").first.text()
	  }
	  catch {
		case e: Exception => ""
	  }
	}

	  val warcc = warcf.
	  	filter{ _._2.header.warcTypeIdx == 2 /* response */ }.
	  	filter{ _._2.getHttpHeader().contentType.startsWith("text/html") }.
	  	map{wr => ( wr._2.header.warcTargetUriStr, HTML2Txt(getContent(wr._2)) )}.cache()
	  
	println("krijg ik iets")
	}
}
