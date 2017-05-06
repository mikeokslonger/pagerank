import networkx as nx
import itertools
import unittest


def pagerank_edgetypes_indirect(D, edgetype_scale, max_iter=100, tol=1.0e-6, weight='weight'):
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
                if W[n][nbr].get('indirect', False):
                    for nbr_adj in W[nbr]:
                        x[nbr_adj] += xlast[n] * W[n][nbr][weight] * edgetype_scale[W[n][nbr]['type']] / len(W[nbr])
                else:
                    x[nbr] += xlast[n] * W[n][nbr][weight] * edgetype_scale[W[n][nbr]['type']]
            x[n] += undistributed_weight * p.get(n, 0)

        err = sum([abs(x[n] - xlast[n]) for n in x])
        if err < N * tol:
            return x
    raise nx.PowerIterationFailedConvergence(max_iter)


def test():
    g = nx.DiGraph()
    edges = [('A', 'area1', {'type': 'area'}),
             ('B', 'area1', {'type': 'area'}),
             ('C', 'area1', {'type': 'area'}),
             ('D', 'area1', {'type': 'area'}),
             ('E', 'area1', {'type': 'area'}),
             ('F', 'area2', {'type': 'area'}),
             ('G', 'area2', {'type': 'area'})]
    #edgetype_scale = {from_node, to_node, attr for }
    edge_types = set([attr['type'] for (_, _, attr) in edges])
    edge_type_scale = {t: 1.0 / len([e for e in edges if e[2]['type'] == t]) for t in edge_types}
    nodes_by_area = [[n[0] for n in v] for k, v in itertools.groupby(edges, lambda x: x[1])]
    indirect_relations = [r for a in nodes_by_area for r in list(itertools.permutations(a, 2))]
    new_edges = [(e[0], e[1], {'type': 'area'}) for e in indirect_relations]
    g.add_edges_from(new_edges)
    r = pagerank_edgetypes_indirect(g, edge_type_scale)
    print r

g = nx.DiGraph()
edges = [('A', 'area1', {'type': 'area', 'indirect': True}),
         ('B', 'area1', {'type': 'area', 'indirect': True}),
         ('C', 'area1', {'type': 'area', 'indirect': True}),
         ('D', 'area1', {'type': 'area', 'indirect': True}),
         ('E', 'area1', {'type': 'area', 'indirect': True}),
         ('F', 'area2', {'type': 'area', 'indirect': True}),
         ('G', 'area2', {'type': 'area', 'indirect': True})]
bidirectional_edges = edges + [(e[1], e[0], e[2]) for e in edges]
edge_types = set([attr['type'] for (_, _, attr) in edges])
edge_type_scale = {t: 1.0 / len([e for e in edges if e[2]['type'] == t]) for t in edge_types}
g.add_edges_from(bidirectional_edges)
r = pagerank_edgetypes_indirect(g, edge_type_scale)
print r