/*import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import scala.reflect.ClassTag

object Pregel extends Logging{

  def aggregateMessagesWithActiveSet[A: ClassTag](
                                                   sendMsg: EdgeContext[VD, ED, A] => Unit,
                                                   mergeMsg: (A, A) => A,
                                                   tripletFields: TripletFields,
                                                   activeSetOpt: Option[(VertexRDD[_], EdgeDirection)])
  : VertexRDD[A]

  def mapReduceTriplets[VD: ClassTag, ED: ClassTag, A: ClassTag](
                                                                  g: Graph[VD, ED],
                                                                  mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
                                                                  reduceFunc: (A, A) => A,
                                                                  activeSetOpt: Option[(VertexRDD[_], EdgeDirection)] = None): VertexRDD[A] = {
    def sendMsg(ctx: EdgeContext[VD, ED, A]) {
      mapFunc(ctx.toEdgeTriplet).foreach { kv =>
        val id = kv._1
        val msg = kv._2
        if (id == ctx.srcId) {
          ctx.sendToSrc(msg)
        } else {
          assert(id == ctx.dstId)
          ctx.sendToDst(msg)
        }
      }
    }
    g.aggregateMessagesWithActiveSet(
      sendMsg, reduceFunc, TripletFields.All, activeSetOpt)
  }

  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED], initialMsg: A, maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A)=> VD,
   sendMsg: EdgeTriplet[VD,ED] => Iterator[(VertexId,A)],
   mergeMsg: (A,A) => A): Graph[VD, ED] ={
    require(maxIterations >= 0, s"Maximum number of iterations must be greater than or equal to 0, " +
      s"but got ${maxIterations}")
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
    //Compute Msg
    var messages = mapReduceTriplets(g, sendMsg, mergeMsg)
    var activeMessages = messages.count()
    //Loop
    var prevG: Graph[VD,ED] = null
    var i =0
    while (activeMessages >0 && i < maxIterations){
      prevG = g
      g = g.joinVertices(messages)(vprog).cache()
      val oldMessages = messages

      messages = mapReduceTriplets(g, sendMsg, mergeMsg, Some((oldMessages, activeDirection))).cache()
      activeMessages = messages.count()

      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.unpersist(blocking = false)
      //Count iteration
      i+=1
      logInfo("Pregel finished iteration" + i)
    }
    messages.unpersist(blocking = false)
    g
  }
}*/
