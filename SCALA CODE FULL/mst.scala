import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar
object PageRank{
class SGraph[T] {
  type Vertex = T
  type GraphMap = Map[Vertex,List[Vertex]]
  var g:GraphMap = Map()
def DFS(start: Vertex): List[Vertex] = {

    def DFS0(v: Vertex, visited: List[Vertex]): List[Vertex] = {     
        val neighbours:List[Vertex] = g(v) filterNot visited.contains
        neighbours.foldLeft(v :: visited)((b,a) => DFS0(a,b))
    }
    DFS0(start,List()).reverse 
  }
}
	
def minSpanningTree[VD:scala.reflect.ClassTag](g:Graph[VD,Double]) = {
var g2 = g.mapEdges(e => (e.attr,false))
for (i <- 1L to g.vertices.count-1) 
{
 val unavailableEdges = g2.outerJoinVertices(g2.subgraph(_.attr._2).connectedComponents.vertices)((vid,vd,cid) => (vd,cid))
.subgraph(et => et.srcAttr._2.getOrElse(-1) ==et.dstAttr._2.getOrElse(-2)).edges.map(e => ((e.srcId,e.dstId),e.attr))

type edgeType = Tuple2[Tuple2[VertexId,VertexId],Double]
val smallestEdge =g2.edges.map(e => ((e.srcId,e.dstId),e.attr)).leftOuterJoin(unavailableEdges).filter(x => !x._2._1._2 && x._2._2.isEmpty)
.map(x => (x._1, x._2._1._1)).min()(new Ordering[edgeType]() 
{
	override def compare(a:edgeType, b:edgeType) = 
	{
		val r = Ordering[Double].compare(a._2,b._2)
			if (r == 0)
				Ordering[Long].compare(a._1._1, b._1._1)
			else
				r
	}
})
g2 = g2.mapTriplets(et => (et.attr._1, et.attr._2 || (et.srcId == smallestEdge._1._1 && et.dstId == smallestEdge._1._2)))
}
g2.subgraph(_.attr._2).mapEdges(_.attr._1)
}

def main(args: Array[String]) {
val conf = new SparkConf().setAppName("Simple Application")
val sc = new SparkContext(conf)
val start1= System.currentTimeMillis

val edges=Array(Edge(1,2,14.0), Edge(1,3,07.0), Edge(1,4,02.0), Edge(1,5,12.0), Edge(1,6,04.0), Edge(1,7,09.0), Edge(1,8,06.0), Edge(2,3,10.0), Edge(2,4,14.0), Edge(2,5,11.0), Edge(2,6,10.0), Edge(2,7,06.0), Edge(2,8,08.0), Edge(3,4,10.0), Edge(3,5,07.0), Edge(3,6,05.0), Edge(3,7,09.0), Edge(3,8,12.0), Edge(4,5,13.0), Edge(4,6,07.0), Edge(4,7,03.0), Edge(4,8,07.0), Edge(5,6,04.0), Edge(5,7,03.0), Edge(5,8,10.0), Edge(6,7,11.0), Edge(6,8,12.0), Edge(7,8,04.0))

val myVertices = sc.makeRDD(Array((1L,"1"), (2L,"2"), (3L,"3"), (4L,"4"), (5L,"5"), (6L,"6"), (7L,"7"), (8L,"8")))
val myEdges = sc.makeRDD(edges)

val myGraph = Graph(myVertices, myEdges)
val mynodes=minSpanningTree(myGraph).triplets.map(et =>(et.srcAttr,et.dstAttr)).collect()
val stop1= System.currentTimeMillis

var newEdge=Array(List(1), List(2), List(3), List(4), List(5), List(6), List(7), List(8))
val i=0
val j=0

for(i <- 0 to 6)
{
	val x= mynodes(i).toString()			
	println(x)

	var vertex1=""
        var edge1=""
        var comma=0
        if(x.charAt(2)==',')
        {
            vertex1=x.substring(1,2)
            comma= 2
            if(x.length()==5)
            {   
                edge1=x.substring(3,4)
            }
            else
                edge1=x.substring(3,5)
        }
        else 
        {
            comma=3
            vertex1=x.substring(1,3)
            if(x.length()==6)
                edge1=x.substring(4,5)
            else
                edge1=x.substring(4,6)
        }
	val edge=edge1.toInt
	val vertex=vertex1.toInt

	print(x+" and "+vertex)				
	val list= newEdge(vertex-1)			

	print("   edge: "+edge)				
	
	val list1= edge::list				
	newEdge(vertex-1)=list1				
	print("list1 updated: "+newEdge(vertex-1))	

	val list4= newEdge(edge-1)
	val list2 =vertex:: list4
	newEdge(edge-1)= list2
	println("list2 updated: "+newEdge(edge-1))
}
for(j <-0 to 6)
{
	newEdge(j)=newEdge(j).filter(_!=(j+1))
	println(newEdge(j))
}

val start2= System.currentTimeMillis
var sGraph= new SGraph[Int]
sGraph.g = Map(1 -> newEdge(0), 2-> newEdge(1), 3-> newEdge(2), 4-> newEdge(3),5 -> newEdge(4), 6-> newEdge(5), 7-> newEdge(6), 8-> List())
print(sGraph.DFS(1))

val stop2=System.currentTimeMillis
println("Total time: "+(stop1-start1+stop2-start2))
}
}
