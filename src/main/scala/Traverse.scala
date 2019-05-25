import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.{OrientGraphFactory, OrientVertex}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}

import scala.collection.mutable
import scala.util.control.Breaks._

object Traverse {

  def traverse(id:Long, maxDepth:Int, limit:Int):Graph[(Short,Float),Byte] = {
    var graph: Graph[(Short, Float), Byte] = null

    val uri: String = "plocal:database/demographdb"
    val factory: OrientGraphFactory = new OrientGraphFactory(uri)
    val ograph = factory.getTx

    try {
      //Lấy Paper.id của index
      val index = factory.getDatabase.getMetadata.getIndexManager.getIndex("Paper.id")
      //Lấy rid của đỉnh xuất phát
      val rid = index.get(id)
      //Nếu có bài báo id
      if (rid != null) {
        //vertices chứa các đỉnh đã duyệt
        val vertices = mutable.Set[OrientVertex]()
        //edges chứa các cạnh đã duyệt
        val edges = mutable.Set[Edge[Byte]]()
        //Lấy đỉnh xuất phát
        var vertex = ograph.getVertex(rid)
        //Thêm đỉnh xuất phát vào vertices
        vertices.add(vertex)
        var count = 1 //Đếm số đỉnh trong vertices
        var depth = 0 //Độ sâu hiện tại
        //Thêm (đỉnh xuất phát,độ sâu hiện tại) vào hàng đợi q
        val q = mutable.Queue((vertex, depth))
        breakable {
          //Trong khi hàng đợi q khác rỗng
          while (!q.isEmpty) {
            //Lấy đỉnh trong hàng đợi q ra duyệt
            val element = q.dequeue
            vertex = element._1
            depth = element._2
            //Nếu đã đến độ sâu tối đa thì ngưng
            if (depth == maxDepth) break
            depth += 1
            //Đọc các đỉnh ở độ sâu kế tiếp
            vertex.getVertices(Direction.BOTH).forEach({ case (v: OrientVertex) => {
              //Thêm cạnh đã duyệt vào edges
              edges.add(Edge(v.getProperty[Long](
              "id"), vertex.getProperty[Long]("id"), 0.toByte) )
              //Nếu chưa duyệt v
              if (!vertices.contains(v)) {
                q.enqueue((v, depth))
                vertices.add(v)
                count += 1
                //Nếu đã lấy được số đỉnh tối đa thì ngưng
                if (count == limit) break
              }
            }
            }) //foreach
          } //while
        }
        //breakable
        // Tạo đối tượng SparkContext
        val sc: SparkContext = SparkContext.getOrCreate()
        ////Tạo vertexRDD và edgeRDD
        val vertexRDD = sc.parallelize(vertices.map(v =>
          (v.getProperty[Long]("id"), (v.getProperty[Short]("year"),1:Float))).toList)
        val edgeRDD = sc.parallelize(edges.toList)
        //Tạo graph
        graph = Graph(vertexRDD, edgeRDD)
      }
    }
    finally {
      ograph.shutdown
    }
    graph
  }//def
}
