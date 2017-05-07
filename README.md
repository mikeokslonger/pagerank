Overview
-

- Contains an implementation of PageRank with support for classifying edges and having the rank of a
vertex depend on which type of edge it's linked from.
    - Consider classifying the social network in a town where multiple different types
      of connections between people are taken into consideration. Connections may be of varying
      importance, being in the same family, as well as sharing the same zip code could both be
      factors but maybe not of equal importance when deciding how interconnected people are.
      
 

Indirect node
-

- In order to reduce the number of edges required, a concept of indirect or cluster nodes has been introduced.
- The purpose of an indirect node is to allow multiple different nodes to share a connection without being
  directly linked.
- An example could be connecting every person in a zip code. Instead of creating N!/(N!-2) edges,
  people could be connected to an indirect zip code node which will not retain any rank when
  running pagerank.
  
Algorithm
-
- Two versions of the algorithm exists, one that includes indirect nodes (pagerank_edgetypes_indirect),
  and one which also supports different node types.
