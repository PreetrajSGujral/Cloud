import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar

object PageRank{


def minSpanningTree[VD:scala.reflect.ClassTag](g:Graph[VD,Double]) = {
var g2 = g.mapEdges(e => (e.attr,false))
for (i <- 1L to g.vertices.count-1) {
val unavailableEdges =
g2.outerJoinVertices(g2.subgraph(_.attr._2)
.connectedComponents
.vertices)((vid,vd,cid) => (vd,cid))
.subgraph(et => et.srcAttr._2.getOrElse(-1) ==
et.dstAttr._2.getOrElse(-2))
.edges
.map(e => ((e.srcId,e.dstId),e.attr))
type edgeType = Tuple2[Tuple2[VertexId,VertexId],Double]
val smallestEdge =
g2.edges
.map(e => ((e.srcId,e.dstId),e.attr))
.leftOuterJoin(unavailableEdges)
.filter(x => !x._2._1._2 && x._2._2.isEmpty)
.map(x => (x._1, x._2._1._1))
.min()(new Ordering[edgeType]() {
override def compare(a:edgeType, b:edgeType) = {
val r = Ordering[Double].compare(a._2,b._2)
if (r == 0)
Ordering[Long].compare(a._1._1, b._1._1)
else
r
}
})
g2 = g2.mapTriplets(et =>
(et.attr._1, et.attr._2 || (et.srcId == smallestEdge._1._1
&& et.dstId == smallestEdge._1._2)))
}
g2.subgraph(_.attr._2).mapEdges(_.attr._1)
}



def main(args: Array[String]) {
val conf = new SparkConf().setAppName("Simple Application")
val sc = new SparkContext(conf)
val start= System.currentTimeMillis

val edges=Array(Edge(1,2,14.0), Edge(1,3,07.0), Edge(1,4,02.0), Edge(1,5,12.0), Edge(1,6,04.0), Edge(1,7,09.0), Edge(1,8,06.0), Edge(2,3,10.0), Edge(2,4,14.0), Edge(2,5,11.0), Edge(2,6,10.0), Edge(2,7,06.0), Edge(2,8,08.0), Edge(3,4,10.0), Edge(3,5,07.0), Edge(3,6,05.0), Edge(3,7,09.0), Edge(3,8,12.0), Edge(4,5,13.0), Edge(4,6,07.0), Edge(4,7,03.0), Edge(4,8,07.0), Edge(5,6,04.0), Edge(5,7,03.0), Edge(5,8,10.0), Edge(6,7,11.0), Edge(6,8,12.0), Edge(7,8,04.0))

val myVertices = sc.makeRDD(Array((1L,"1"), (2L,"2"), (3L,"3"), (4L,"4"), (5L,"5"), (6L,"6"), (7L,"7"), (8L,"8")))
val myEdges = sc.makeRDD(edges)

val myGraph = Graph(myVertices, myEdges)
val mynodes=minSpanningTree(myGraph).triplets.map(et =>(et.srcAttr,et.dstAttr)).collect()
println(mynodes)

var newEdge=Array(Edge(1,1,02.0))
val i=0
for(i <- 0 to 6)
{
	val x= mynodes(i).toString()
	val y=x.substring(0,4)

	var a=0
	for( a<- edges)
	{

		val p=a.toString().substring(4,8)
		if(p==y)
		{
    			println("	String is: "+p+"  edge given is: "+y)
    			val edge= Edge(y(1).toInt, y(3).toInt, a.toString().substring(9,12).toDouble)  	
    			newEdge +:= edge
		}
	}
}
println("size of array of edges is: "+newEdge.size)
//convert edges to strings..... if the mst contains that edge, create a new edge array and add that edge there 

println("")
val stop=System.currentTimeMillis
println("Total time: "+(stop-start))
}
}
