import com.tinkerpop.blueprints.impls.orient._
import com.orientechnologies.orient.core.metadata.schema._
import com.orientechnologies.orient.core.sql._
import com.tinkerpop.blueprints.{Direction, Edge, Vertex}


import scala.collection.JavaConverters._

object TestGraph extends App {
  val WorkEdgeLabel = "Work"

  // opens the DB (if not existing, it will create it)
  val uri: String = "plocal:target/database/test"
  val factory: OrientGraphFactory = new OrientGraphFactory(uri)
  val graph: OrientGraph = factory.getTx()

  try {

    // if the database does not contain the classes we need (i.e. it was just created),
    // then adds them
    if (graph.getVertexType("Person") == null) {

      // we now extend the Vertex class for Person and Company
      val person: OClass = graph.createVertexType("Person")
      person.createProperty("firstName", OType.STRING)
      person.createProperty("lastName", OType.STRING)

      val company: OClass = graph.createVertexType("Company")
      company.createProperty("name", OType.STRING)
      company.createProperty("revenue", OType.LONG)

      val product: OClass = graph.createVertexType("Project")
      product.createProperty("name", OType.STRING)

      // we now extend the Edge class for a "Work" relationship
      // between Person and Company
      val work: OClass = graph.createEdgeType(WorkEdgeLabel)
      work.createProperty("startDate", OType.DATE)
      work.createProperty("endDate", OType.DATE)
      work.createProperty("projects", OType.LINKSET)
    }
    else {

      // cleans up the DB since it was already created in a preceding run
      graph.command(new OCommandSQL("DELETE VERTEX V")).execute()
      graph.command(new OCommandSQL("DELETE EDGE E")).execute()
    }

    // adds some people
    // (we have to force a vararg call in addVertex() method to avoid ambiguous
    // reference compile error, which is pretty ugly)
    val johnDoe: Vertex = graph.addVertex("class:Person", Nil: _*)
    johnDoe.setProperty("firstName", "John")
    johnDoe.setProperty("lastName", "Doe")

    // we can also set properties directly in the constructor call
    val johnSmith: Vertex = graph.addVertex("class:Person", "firstName", "John", "lastName", "Smith")
    val janeDoe: Vertex = graph.addVertex("class:Person", "firstName", "Jane", "lastName", "Doe")

    // creates a Company
    val acme: Vertex = graph.addVertex("class:Company", "name", "ACME", "revenue", "10000000")

    // creates a couple of projects
    val acmeGlue = graph.addVertex("class:Project", "name", "ACME Glue")
    val acmeRocket = graph.addVertex("class:Project", "name", "ACME Rocket")

    // creates edge JohnDoe worked for ACME
    val johnDoeAcme: Edge = graph.addEdge(null, johnDoe, acme, WorkEdgeLabel)
    johnDoeAcme.setProperty("startDate", "2010-01-01")
    johnDoeAcme.setProperty("endDate", "2013-04-21")
    johnDoeAcme.setProperty("projects", Set(acmeGlue, acmeRocket))

    // another way to create an edge, starting from the source vertex
    val johnSmithAcme: Edge = johnSmith.addEdge(WorkEdgeLabel, acme)
    johnSmithAcme.setProperty("startDate", "2009-01-01")

    // prints all the people who works/worked for ACME
    val res: OrientDynaElementIterable = graph
      .command(new OCommandSQL(s"SELECT expand(in('${WorkEdgeLabel}')) FROM Company WHERE name='ACME'"))
      .execute()

    println("ACME people:")
    res.forEach(v => {

      // gets the person
      val person = v.asInstanceOf[OrientVertex]

      // gets the "Work" edge
      val workEdgeIterator = person.getEdges(Direction.OUT, WorkEdgeLabel).iterator()
      val edge = workEdgeIterator.next()

      // and retrieves info to print
      val status = if (edge.getProperty("endDate") != null) "retired" else "active"
      val projects = if (edge.getProperty("projects") != null)
        edge.getProperty("projects").asInstanceOf[Set[Vertex]].map(v=>v.getProperty[String]("name")).mkString(", ") else "Any"

      println(s"Name: ${person.getProperty("lastName")}, ${person.getProperty("firstName")} [${status}]. Worked on: ${projects}.")
    })
  }
  finally {
    graph.shutdown()
  }
}
