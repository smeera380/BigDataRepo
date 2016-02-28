package com.examples

import scala.xml.{XML, NodeSeq}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import scala.io.Source

object GraphxPageRank {

  def parseLine(line: String): (String,Array[String])={
    val splits = line.split("\t")
    
    try{
	  var xmlStr = "" 
    	  xmlStr = splits(3).replace("\\n","")
          //println("@@@@@@@@@@@@@@@@@@@@@@@@")
          //println(xmlStr)
  
          var xmlStr1 = xmlStr.substring(xmlStr.indexOf("<articles"),xmlStr.length())    
          var xmlFormat = xml.XML.loadString(xmlStr1)    
          var xmlLinks = (xmlFormat \\ "target").map(_.text).toArray
           //.map(_.text)
 	  //var outLinks  = (xmlLinks.map(_.toString)).toArray
          //println("#######################   XML LInks !!!  "+ xmlLinks)

          (splits(1),xmlLinks)

    }
    catch{
        case e: Exception =>  
		{  println(" Big string ")
                   (splits(1),Array())  }       
    }

  }


  def assignHash(title: String): VertexId = {
      title.toLowerCase.replace(" ", "").hashCode.toLong
    }

  def main(args: Array[String]) {
   
  
    if (args.length < 1) {
      System.err.println("Usage: GraphxPageRank <filename> <inputdataset> <numofiterations>")
      System.exit(-1)
    }

    val conf = new SparkConf().setAppName("GraphXPageRank")
    val sc = new SparkContext(conf)

    //val lines = sc.textFile("/home/smeera380/spark-1.6.0/freebase-wex-2010-07-05-articles1.tsv")

    println("@@@@@@@@@@@@@@@@@@  Arguments : ")
    //println(args(0) + ":"+ args(1) )

    val wiki = sc.textFile("s3n://smeeras301/wexdataset")

    //Parse the WEX dataset to Article name and the Outlinks from the Article
    val links = wiki.map(parseLine _).cache()
    //links.saveAsTextFile("/home/smeera380/spark-1.6.0/PageRankOutput")

    // Creating the Vertices RDD using a Hash function to denote the Vertex Id and the Page name obtained from the above RDD.
    val verticesRDD = links.map(art => (assignHash(art._1),art._1)).cache()
    //verticesRDD.saveAsTextFile("/home/smeera380/spark-1.6.0/PageRankOutput")
    
    // Creating the Edges RDD using the Hash function to SourceId, DestinationId and Initial weight set to 1.0 
    val edgesRDD: RDD[Edge[Double]] = links.flatMap { art =>
      val srcid = assignHash(art._1)
      art._2.map(ol => {
        val destId = assignHash(ol)
        Edge(srcid, destId, 1.0)
      })
    }
    //edgesRDD.saveAsTextFile("/home/smeera380/spark-1.6.0/PageRankOutput")

    // Creating the Graph
    val graph = Graph(verticesRDD,edgesRDD,"").subgraph(vpred = {(v, d) => d.nonEmpty}).cache
 
    //Calculating the Pagerank - Number of iterations used - 8
    val prg = graph.staticPageRank(args(0).toInt).cache 
    
    //Linking the Article title with the computed rank
    var finalPgGrph = graph.outerJoinVertices(prg.vertices) {
     (v, article, pageRank) => (pageRank.getOrElse(0.0), article)
    }

    finalPgGrph.vertices.top(100){
       Ordering.by((entry:(VertexId,(Double,String))) => entry._2._1)
    }.foreach(t=> println(t._2._2 + ": " + t._2._1))
    
    //finalPgGrph.vertices.saveAsTextFile("/home/smeera380/spark-1.6.0/PageRankOutput")
    //finalPgGrph.edges.saveAsTextFile("/home/smeera380/spark-1.6.0/PageRankOutput")

    /*
    val edgesRDD: RDD[Edge[Double]] = links.flatMap {
      art => (assignHash(art._1), art._2.map(ol=> {assignHash(art._2),1.0}))
    }
    edgesRDD.saveAsTextFile("/home/smeera380/spark-1.6.0/PageRankOutput")

    */

    sc.stop()
    
  }

}
