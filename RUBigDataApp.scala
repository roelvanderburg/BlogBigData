package org.rubigdata

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import nl.surfsara.warcutils.WarcInputFormat
import org.jwat.warc.WarcConstants
import org.jwat.warc
import org.apache.hadoop.io.LongWritable

import org.apache.commons.lang.StringUtils

import org.jsoup.Jsoup
import java.io.InputStreamReader
import java.io.IOException


object RUBigDataApp extends Serializable{
  def getContent(record: warc.WarcRecord) = {
    val cLen = record.header.contentLength.toInt
    val cStream = record.getPayload.getInputStream()
    val content = new java.io.ByteArrayOutputStream();

    val buf = new Array[Byte](cLen)

    var nRead = cStream.read(buf)
    while (nRead != -1) {
      content.write(buf, 0, nRead)
      nRead = cStream.read(buf)
    }
    cStream.close()
    try {
      Jsoup.parse(content.toString("UTF-8")).text().replaceAll("[\\r\\n]+", " ")
    }
    catch {
      case e: Exception => throw new IOException("Caught exception processing input row ", e)
    }
  }

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("RUBigDataAppAllWarcOnSegment")
   
    
    
    
    val warcfile = "/data/public/common-crawl/crawl-data/CC-MAIN-2016-07/segments/1454701167599.48/warc/*"
    val sc = new SparkContext(conf)

    val warcf = sc.newAPIHadoopFile(
                warcfile,
                classOf[WarcInputFormat],               // InputFormat
                classOf[LongWritable],                  // Key
                classOf[warc.WarcRecord]                     // Value
      )

    val warcc = warcf.
      filter{ _._2.header.warcTypeIdx == 2 /* response */ }.
      filter{ _._2.getHttpHeader().statusCode == 200 }.
      filter{ _._2.getHttpHeader().contentType != null}.
      filter{ _._2.getHttpHeader().contentType.startsWith("text/html") }.
      filter{ _._2 != null}.
      map{wr => ( wr._2.header.warcTargetUriStr, getContent(wr._2) )}.cache()
    
    val politicianlist: List[String] = List("rutte", "wilders", "pechtold","buma","thieme","klaver","baudet","roemer","asscher","krol")
    
    // Transform into word and count.

    val words = warcc.flatMap(line => line._2.split(" ")).filter(wr => politicianlist.contains(wr))
       
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}.sortBy(x => -x._2)    
    counts.take(10).foreach(tuple=>println(tuple))
  }

}
