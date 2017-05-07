import org.apache.spark.graphx.lib._
import org.apache.spark.graphx._
import scala.reflect.ClassTag

case class SuperEdge[T](attr: T, weight: Double, edgeType: String)

def runWithOptions[VD: ClassTag, ED: ClassTag](graph: Graph[VD, SuperEdge[ED]], edgeTypeScale: Map[String, Double], numIter: Int): Graph[Double, SuperEdge[Double]] =
  {
    val resetProb = 0.15
    val src: VertexId = -1L

    val N = graph.vertices.count
    var rankGraph = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      .mapTriplets( e => SuperEdge[Double](1.0 / e.srcAttr, 0, e.attr.edgeType), TripletFields.Src )
      .mapVertices { (id, attr) => 1.0 / N}

    val edgesBySourceVertex = rankGraph.edges.groupBy(e => e.srcId).join(rankGraph.vertices).map{case (fromId, (edges, vertexRank)) => (fromId, vertexRank, edges)}
    val weightToDistribute = edgesBySourceVertex.map{case (fromId, vertexRank, edges) => edges.map(e => vertexRank * e.attr.attr * edgeTypeScale(e.attr.edgeType) ).sum  }.reduce(_ + _)
    val undistributedWeight = 1 - weightToDistribute

    var iteration = 0
    while (iteration < numIter) {
      rankGraph.cache()

      val weightContributions = edgesBySourceVertex.flatMap{case (fromId, vertexRank, edges) => edges.map(e => (e.dstId, vertexRank * e.attr.attr * edgeTypeScale(e.attr.edgeType)))}
      val weightContributionsByVertex = weightContributions.groupBy(_._1).map{case (vertex, contributions) => (vertex, contributions.map(_._2).sum)}

      rankGraph = rankGraph
        .mapVertices[Double]((vid, data) => undistributedWeight / N)
        .joinVertices(weightContributionsByVertex){(vertexId, left, right) => left + right}

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      iteration += 1
    }
    rankGraph
  }
