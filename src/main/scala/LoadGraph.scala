
import com.orientechnologies.orient.core.db.document.{ODatabaseDocumentTx}
import com.orientechnologies.orient.core.id.{ORID, ORecordId}
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.{OConcurrentLegacyResultSet, OSQLSynchQuery}
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

object LoadGraph {


  def loadGraph(pageSize: Int, sc: SparkContext): Graph[(Short,Float),Byte] = {

    var graph: Graph[(Short,Float),Byte] = null

    val uri: String = "plocal:/Users/zNedu/Desktop/orientdb-3.0.10/databases/New"
    //"remote:localhost/New"

    val factory: OrientGraphFactory = new OrientGraphFactory(uri)
    val ograph = factory.getTx

    val db: ODatabaseDocumentTx = ograph.getRawGraph

    try{

      //Tạo VertexRDD
      var vertexRDD: RDD[(Long,(Short,Float))] = sc.emptyRDD
      //Đọc các đỉnh theo từng trang
      var query: OSQLSynchQuery[ODocument] = new OSQLSynchQuery[ODocument](
        s"select from Paper where @rid > ? LIMIT ${pageSize}")
      var resultSet: OConcurrentLegacyResultSet[ODocument] = db.query(
        query,new ORecordId()
      )

      resultSet.forEach(document =>{
        val last: ORID = resultSet.get(resultSet.size() - 1).getIdentity()
        val vId = document.getProperty("id"):Long
        val vYear = document.getProperty("year"):Short
        //val list = Array().toList
        vertexRDD = vertexRDD ++ sc.parallelize(Seq((vId, (vYear, 1:Float))))

        //vertexRDD = document.asVertex().map()
        resultSet = db.query(query, last)
      })

      //vertexRDD = vertexRDD ++ sc.parallelize()

      /*while (!resultSet.isEmpty()){
        val last: ORID = resultSet.get(resultSet.size() - 1).getIdentity()
        val list = resultSet.map(v => (v[Long]("id"),(v[Short]("year"),1:Float))).toList
        vertexRDD = vertexRDD ++ sc.parallelize(list)
        resultSet = db.query(query, last)
      }*/

      //Tạo edgeRDD
      var edgeRDD: RDD[Edge[Byte]] = sc.emptyRDD
      //Đọc các cạnh theo từng trang
      query = new OSQLSynchQuery[ODocument](
        s"select from Reference where @rid > ? LIMIT ${pageSize}"
      )
      resultSet = db.query(query, new ORecordId())

      resultSet.forEach(document =>{
        val last: ORID = resultSet.get(resultSet.size() - 1).getIdentity()
        val eSrc = document.getProperty("src"):Long
        val eDst = document.getProperty("dst"):Long
        edgeRDD = edgeRDD ++ sc.parallelize(Seq(Edge(eSrc, eDst, 0:Byte)))
        resultSet = db.query(query,last)

      })

      /*while (!resultSet.isEmpty()){
        val last: ORID = resultSet.get(resultSet.size() - 1).getIdentity()
        val list = resultSet.map(e => Edge(e.field[Long]("src"),e.field[Long]("dst"),0.toByte)).toList
        edgeRDD = edgeRDD ++ sc.parallelize(list)
        resultSet = db.query(query,last)
      }*/

      //Tạo Graph
      graph = Graph(vertexRDD,edgeRDD)
    }
    finally {
      ograph.shutdown()
    }
    graph
  }
}
