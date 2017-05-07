import org.apache.spark.graphx.lib._
import org.apache.spark.graphx._
import scala.reflect.ClassTag

case class SuperEdge[T](attr: T, weight: Double, edgeType: String)

object PageRank {

  def pageRankEdgeTypes[VD: ClassTag, ED: ClassTag](graph: Graph[VD, SuperEdge[ED]], edgeTypeScale: Map[String, Double], numIter: Int): Graph[Double, SuperEdge[Double]] =
  {
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
      println(iteration)
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

  def pageRankEdgeTypesIndirect[VD: ClassTag, ED: ClassTag](graph: Graph[VD, SuperEdge[ED]],
                                                          edgeTypeScale: Map[String, Double],
                                                          indirectNodes: Set[VertexId],
                                                          numIter: Int): Graph[Double, SuperEdge[Double]] =
  {
    val tol = 0.00000001

    val N = graph.vertices.filter{case (vertexId, _) => !indirectNodes.contains(vertexId)}.count
    var rankGraph = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      .mapTriplets( e => SuperEdge[Double](1.0 / e.srcAttr, 0, e.attr.edgeType), TripletFields.Src )
      .mapVertices { (vertexId, attr) => if (indirectNodes.contains(vertexId)) 0.0 else 1.0 / N}

    var edgesBySourceVertex = rankGraph.edges.groupBy(e => e.srcId).join(rankGraph.vertices).map{case (fromId, (edges, vertexRank)) => (fromId, vertexRank, edges)}
    var weightToDistribute = edgesBySourceVertex.map{case (fromId, vertexRank, edges) => edges.map(e => vertexRank * e.attr.attr * edgeTypeScale(e.attr.edgeType) ).sum  }.reduce(_ + _)
    var undistributedWeight = 1 - weightToDistribute

    var iteration = 0
    while (iteration < numIter) {
      rankGraph.cache()
      edgesBySourceVertex.cache()

      val weightContributions = edgesBySourceVertex.flatMap{case (fromId, vertexRank, edges) => edges.map(e => (e.dstId, vertexRank * e.attr.attr * edgeTypeScale(e.attr.edgeType)))}
      val weightContributionsByVertex = weightContributions.groupBy(_._1).map{case (vertex, contributions) => (vertex, contributions.map(_._2).sum)}
      val directWeightContributionsByVertex = weightContributionsByVertex.filter{ case (vertexId, rank) => !indirectNodes.contains(vertexId) }
      val indirectWeightContributionsByVertex = weightContributionsByVertex
        .filter{case (vertexId, rank) => indirectNodes.contains(vertexId)}
        .join(edgesBySourceVertex.map{case (vertexId, rank, edges) => (vertexId, (rank, edges))})
        .flatMap{ case (vertexId, (rank, (_, edges))) => edges.map(e => (e.dstId, rank * e.attr.attr))}

      val allWeightContributionsByVertex = directWeightContributionsByVertex
        .union(indirectWeightContributionsByVertex)
        .groupBy(_._1).map{case (vertex, contributions) => (vertex, contributions.map(_._2).sum)}

      val previousRankGraph = rankGraph
      rankGraph = rankGraph
        .mapVertices[Double]((vertexId, data) => if (indirectNodes.contains(vertexId)) 0.0 else undistributedWeight / N)
        .joinVertices(allWeightContributionsByVertex){(vertexId, left, right) => if (indirectNodes.contains(vertexId)) 0.0 else left + right}

      val diff = previousRankGraph.vertices.join(rankGraph.vertices).map{case (vertexId, (last, now)) => Math.abs(last - now)}.collect.sum
      println("Iteration: %s, Diff: %s".format(iteration, diff))
      if(diff < N * tol){
        println("Done")
        return rankGraph
      }

      edgesBySourceVertex = rankGraph.edges.groupBy(e => e.srcId).join(rankGraph.vertices).map{case (fromId, (edges, vertexRank)) => (fromId, vertexRank, edges)}
      weightToDistribute = edgesBySourceVertex.map{case (fromId, vertexRank, edges) => edges.map(e => vertexRank * e.attr.attr * edgeTypeScale(e.attr.edgeType) ).sum  }.reduce(_ + _)
      undistributedWeight = 1 - weightToDistribute

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      previousRankGraph.vertices.unpersist(false)
      previousRankGraph.edges.unpersist(false)
      iteration += 1
    }
    rankGraph
  }
}