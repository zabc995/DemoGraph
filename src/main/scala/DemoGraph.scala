import com.orientechnologies.orient.core.metadata.schema.{OClass, OType}
import com.tinkerpop.blueprints.impls.orient._
import com.tinkerpop.blueprints.{Edge, Parameter, Vertex}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.io.Source

object DemoGraph extends App {

  def prepareGraph(graph: OrientGraph): Unit ={
    val paper: OClass = graph.createVertexType("Paper")
    paper.createProperty("id", OType.LONG)
    paper.createProperty("title", OType.STRING)
    paper.createProperty("authors", OType.STRING)
    paper.createProperty("year", OType.SHORT)
    paper.createProperty("publicationVenue", OType.STRING)
    paper.createProperty("index", OType.STRING)
    paper.createProperty("abstract", OType.STRING)
    paper.createProperty("references", OType.STRING)

    graph.createKeyIndex("index", classOf[Vertex], new Parameter("class", "Paper"))
    val reference: OClass = graph.createEdgeType("Reference")
    reference.createProperty("src", OType.LONG)
    reference.createProperty("dst", OType.LONG)
  }

  def importNodes(graph: OrientGraph, fileName: String): Unit ={
    val source = Source.fromFile(fileName)
    var paper: Vertex = null
    var count = 0


    for(line <- source.getLines()){
      if(line.contains("#*")){
        count += 1
        println(count)
        paper = graph.addVertex("class:Paper",Nil:_*)
        paper.setProperty("id", count)
        paper.setProperty("title", line.substring(2).trim)
        if(count % 100 == 0) graph.commit()
      }
      else if(line.contains("#@")){
        paper.setProperty("authors", line.substring(2).trim)
      }
      else if(line.contains("#t")){
        paper.setProperty("year", line.substring(2).trim)
      }
      else if(line.contains("#c")){
        paper.setProperty("publicationVenue", line.substring(2).trim)
      }
      else if(line.contains("#index")){
        paper.setProperty("index", line.substring(6).trim)
      }
      else if(line.contains("#!")){
        paper.setProperty("abstract", line.substring(2).trim)
      }
      else if(line.contains("#%")){
        var references: String = paper.getProperty[String]("references")
        if(references == null){
          references = ""
        }
        references += line.substring(2).trim + ";"
        paper.setProperty("references", references)
      }
    }
    source.close()
  }

  def createEdges(graph: OrientGraph): Unit ={
    var paper: Vertex = null
    val iterator = graph.getVertices.iterator()
    var count = 0
    while(iterator.hasNext){
      paper=iterator.next()
      if(paper.getProperty("references")!=null){
        val indices = paper.getProperty("references").toString.split(";")
        indices.foreach(refIndex =>{
          val innerIterator = graph.getVertices("Paper.index", refIndex).iterator()
          if(innerIterator.hasNext){
            val originalPaper: Vertex = innerIterator.next()
            val reference: Edge = graph.addEdge(null, paper, originalPaper, "Reference")
            reference.setProperty("src", paper.getProperty[Long]("id"))
            reference.setProperty("dst", originalPaper.getProperty[Long]("id"))
            count += 1
            if(count % 100 == 0)  graph.commit()
          }
        })
      }
    }
  }

  override def main(args: Array[String]): Unit = {
    println("Begin creating the graph ...")

    val uri: String = "plocal:/Users/zNedu/Desktop/orientdb-3.0.10/databases/New"
    val factory: OrientGraphFactory = new OrientGraphFactory(uri)
    factory.setStandardElementConstraints(false)
    val graph: OrientGraph = factory.getTx()

    val sparkSS: SparkSession = SparkSession.builder()
      //.master("local[*]")
      .appName("Spark Graphx App")
      //.config("spark.master","spark://172.16.7.161:7077")
      .getOrCreate()

    val sc: SparkContext = sparkSS.sparkContext


    try{
      prepareGraph(graph)
      importNodes(graph, s"C:\\Users\\zNedu\\IdeaProjects\\Test1\\src\\main\\resources\\outputacm.txt")
      createEdges(graph)
    }
    finally{
      factory.close()
    }

    println("End creating the graph ...!")

    LoadGraph.loadGraph(50000, sc)
    sc.stop()

    println("End Spark Load Graph ...!")

  }
}
