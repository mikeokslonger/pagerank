import networkx as nx
import itertools
import unittest


def pagerank_edgetypes(D, edgetype_scale, max_iter=100, tol=1.0e-6, weight='weight'):
    W = nx.stochastic_graph(D, weight=weight)
    N = W.number_of_nodes()

    x = dict.fromkeys(W, 1.0 / N)
    p = dict.fromkeys(W, 1.0 / N)

    for _ in range(max_iter):
        xlast = x
        x = dict.fromkeys(xlast.keys(), 0)
        weight_to_distribute = sum([(xlast[n] * W[n][nbr]['weight'] * edgetype_scale[W[n][nbr]['type']]) for n in x for nbr in W[n]])
        undistributed_weight = 1 - weight_to_distribute
        for n in x:
            for nbr in W[n]:
                x[nbr] += xlast[n] * W[n][nbr][weight] * edgetype_scale[W[n][nbr]['type']]
            x[n] += undistributed_weight * p.get(n, 0)

        err = sum([abs(x[n] - xlast[n]) for n in x])
        if err < N * tol:
            return x
    raise nx.PowerIterationFailedConvergence(max_iter)


def pagerank_edgetypes_indirect(D, edgetype_scale, indirect_nodes, max_iter=100, tol=1.0e-6, weight='weight'):
    W = nx.stochastic_graph(D, weight=weight)
    N = W.number_of_nodes()

    direct_nodes = [a for a in W if a not in indirect_nodes]
    x = dict.fromkeys(direct_nodes, 1.0 / len(direct_nodes))
    p = dict.fromkeys(direct_nodes, 1.0 / len(direct_nodes))

    for _ in range(max_iter):
        xlast = x
        x = dict.fromkeys(xlast.keys(), 0)
        weight_to_distribute = sum([(xlast[n] * W[n][nbr]['weight'] * edgetype_scale[W[n][nbr]['type']]) for n in x for nbr in W[n]])
        undistributed_weight = 1 - weight_to_distribute
        for n in x:
            for nbr in W[n]:
                if nbr in indirect_nodes:
                    contribution = xlast[n] * W[n][nbr][weight] * edgetype_scale[W[n][nbr]['type']] / len(W[nbr])
                    for nbr_adj in W[nbr]:
                        x[nbr_adj] += contribution
                else:
                    x[nbr] += xlast[n] * W[n][nbr][weight] * edgetype_scale[W[n][nbr]['type']]
            x[n] += undistributed_weight * p.get(n, 0)

        err = sum([abs(x[n] - xlast[n]) for n in x])
        if err < N * tol:
            return x
    raise nx.PowerIterationFailedConvergence(max_iter)


class EdgeTypePageRankTest(unittest.TestCase):

    edgetype_scale = {'area': 0.001,
                      'friend': 0.2}

    def test_same_relation_types(self):
        g = nx.DiGraph()
        g.add_edges_from([('A', 'B', {'type': 'friend'}),
                          ('A', 'C', {'type': 'friend'})])
        r = pagerank_edgetypes(g, EdgeTypePageRankTest.edgetype_scale)
        assert ([k for k, v in sorted(r.iteritems(), key=lambda (k, v): v, reverse=True)] == ['C', 'B', 'A'])

    def test_same_relation_types_normalized_node_weights(self):
        g = nx.DiGraph()
        g.add_edges_from([('A', 'B', {'weight': 1.0, 'type': 'friend'}),
                          ('A', 'C', {'weight': 0.5, 'type': 'friend'})])
        r = pagerank_edgetypes(g, EdgeTypePageRankTest.edgetype_scale)
        assert ([k for k, v in sorted(r.iteritems(), key=lambda (k, v): v, reverse=True)] == ['B', 'C', 'A'])

    def test_multiple_relation_types(self):
        g = nx.DiGraph()
        g.add_edges_from([('A', 'B', {'type': 'friend'}),
                          ('A', 'B', {'type': 'area'}),
                          ('A', 'C', {'type': 'friend'})])
        r = pagerank_edgetypes(g, EdgeTypePageRankTest.edgetype_scale)
        assert ([k for k, v in sorted(r.iteritems(), key=lambda (k, v): v, reverse=True)] == ['C', 'B', 'A'])

    # Would expect the same order as in test_same_relation_types but it's off as
    # B and C are different on the 16th decimal place
    def test_multiple_relation_types_circular(self):
        g = nx.DiGraph()
        g.add_edges_from([('A', 'B', {'type': 'area'}),
                          ('B', 'A', {'type': 'area'}),
                          ('A', 'C', {'type': 'area'}),
                          ('C', 'A', {'type': 'area'}),
                          ('B', 'C', {'type': 'area'}),
                          ('C', 'B', {'type': 'area'}),
                          ('A', 'B', {'type': 'friend'}),
                          ('A', 'C', {'type': 'friend'})])
        r = pagerank_edgetypes(g, EdgeTypePageRankTest.edgetype_scale)
        assert ([k for k, v in sorted(r.iteritems(), key=lambda (k, v): v, reverse=True)] == ['B', 'C', 'A'])

    def test_multiple_relation_types_circular(self):
        g = nx.DiGraph()
        g.add_edges_from([('A', 'B', {'type': 'area'}),
                          ('B', 'A', {'type': 'area'}),
                          ('A', 'C', {'type': 'area'}),
                          ('C', 'A', {'type': 'area'}),
                          ('B', 'C', {'type': 'area'}),
                          ('C', 'B', {'type': 'area'}),
                          ('A', 'B', {'type': 'friend'}),
                          ('A', 'C', {'type': 'friend'})])
        r = pagerank_edgetypes(g, EdgeTypePageRankTest.edgetype_scale)
        assert ([k for k, v in sorted(r.iteritems(), key=lambda (k, v): v, reverse=True)] == ['B', 'C', 'A'])


    def test_multiple_relation_types_circular_relative_edgetype_weights(self):
        g = nx.DiGraph()
        edges = [('A', 'B', {'type': 'area'}),
                 ('B', 'A', {'type': 'area'}),
                 ('A', 'C', {'type': 'area'}),
                 ('C', 'A', {'type': 'area'}),
                 ('B', 'C', {'type': 'area'}),
                 ('C', 'B', {'type': 'area'}),
                 ('A', 'B', {'type': 'friend'}),
                 ('A', 'C', {'type': 'friend'})]
        #edgetype_scale = {from_node, to_node, attr for }
        edge_types = set([attr['type'] for (_, _, attr) in edges])
        edge_type_scale = {t: 1.0 / len([e for e in edges if e[2]['type'] == t]) for t in edge_types}
        g.add_edges_from(edges)
        r = pagerank_edgetypes(g, edge_type_scale)
        assert ([k for k, v in sorted(r.iteritems(), key=lambda (k, v): v, reverse=True)] == ['C', 'B', 'A'])

    def test_multiple_relation_types_circular_multiple_areas(self):
        g = nx.DiGraph()
        edges = [('A', 'B', {'type': 'area'}),
                 ('B', 'A', {'type': 'area'}),
                 ('A', 'E', {'type': 'area'}),
                 ('E', 'A', {'type': 'area'}),
                 ('B', 'E', {'type': 'area'}),
                 ('E', 'B', {'type': 'area'}),
                 ('C', 'D', {'type': 'area'}),
                 ('D', 'C', {'type': 'area'}),
                 ('B', 'C', {'type': 'friend'}),
                 ('B', 'A', {'type': 'friend'})]
        #edgetype_scale = {from_node, to_node, attr for }
        edge_types = set([attr['type'] for (_, _, attr) in edges])
        edge_type_scale = {t: 1.0 / len([e for e in edges if e[2]['type'] == t]) for t in edge_types}
        g.add_edges_from(edges)
        r = pagerank_edgetypes(g, edge_type_scale)
        assert ([k for k, v in sorted(r.iteritems(), key=lambda (k, v): v, reverse=True)] == ['C', 'A', 'D', 'B', 'E'])

    def test_multiple_relation_types_circular_multiple_areas_big(self):
        g = nx.DiGraph()
        edges = [('A', 'B', {'type': 'area'}),
                 ('B', 'A', {'type': 'area'}),
                 ('A', 'C', {'type': 'area'}),
                 ('C', 'A', {'type': 'area'}),
                 ('A', 'D', {'type': 'area'}),
                 ('D', 'A', {'type': 'area'}),
                 ('A', 'J', {'type': 'area'}),
                 ('J', 'A', {'type': 'area'}),
                 ('B', 'C', {'type': 'area'}),
                 ('C', 'B', {'type': 'area'}),
                 ('B', 'D', {'type': 'area'}),
                 ('D', 'B', {'type': 'area'}),
                 ('B', 'J', {'type': 'area'}),
                 ('J', 'B', {'type': 'area'}),
                 ('C', 'D', {'type': 'area'}),
                 ('D', 'C', {'type': 'area'}),
                 ('C', 'J', {'type': 'area'}),
                 ('J', 'C', {'type': 'area'}),
                 ('D', 'J', {'type': 'area'}),
                 ('J', 'D', {'type': 'area'}),
                 ('E', 'F', {'type': 'area'}),
                 ('F', 'E', {'type': 'area'}),
                 ('E', 'G', {'type': 'area'}),
                 ('G', 'E', {'type': 'area'}),
                 ('F', 'G', {'type': 'area'}),
                 ('G', 'F', {'type': 'area'}),
                 ('H', 'I', {'type': 'area'}),
                 ('I', 'H', {'type': 'area'}),
                 ('B', 'A', {'type': 'friend'}),
                 ('B', 'C', {'type': 'friend'}),
                 ('C', 'E', {'type': 'foe'}),
                 ('E', 'B', {'type': 'foe'}),
                 ('D', 'F', {'type': 'friend'}),
                 ('F', 'I', {'type': 'owner'}),
                 ('F', 'H', {'type': 'owner'}),
                 ('G', 'D', {'type': 'rival'}),
                 ]
        # edgetype_scale = {from_node, to_node, attr for }
        edge_types = set([attr['type'] for (_, _, attr) in edges])
        edge_type_scale = {t: 1.0 / len([e for e in edges if e[2]['type'] == t]) for t in edge_types}
        g.add_edges_from(edges)
        r = pagerank_edgetypes(g, edge_type_scale)
        assert ([k for k, v in sorted(r.iteritems(), key=lambda (k, v): v, reverse=True)] ==
                ['D', 'B', 'I', 'H', 'E', 'C', 'A', 'F', 'J', 'G'])

    def test_indirect_graph(self):
        g = nx.DiGraph()
        edges = [('A', 'area1', {'type': 'area'}),
                 ('B', 'area1', {'type': 'area'}),
                 ('C', 'area1', {'type': 'area'}),
                 ('D', 'area1', {'type': 'area'}),
                 ('E', 'area1', {'type': 'area'}),
                 ('F', 'area2', {'type': 'area'}),
                 ('G', 'area2', {'type': 'area'})]
        indirect_nodes = set([e[1] for e in edges])
        bidirectional_edges = edges + [(e[1], e[0], e[2]) for e in edges]
        edge_types = set([attr['type'] for (_, _, attr) in edges])
        edge_type_scale = {t: 1.0 / len([e for e in edges if e[2]['type'] == t]) for t in edge_types}
        g.add_edges_from(bidirectional_edges)
        r = pagerank_edgetypes_indirect(g, edge_type_scale, indirect_nodes)
        assert max([abs(a - b) for (a, b) in itertools.permutations(set(r.values()), 2)]) < 1e-15
        assert sum(r.values()) == 1
        assert set(r.keys()).intersection(set(indirect_nodes)) == set()
    