import networkx as nx

edgetype_scale = {'sector': 0.001,
                  'supplier': 0.2}

def pagerank_edgetypes(D, alpha=0.85, personalization=None,
                       max_iter=100, tol=1.0e-6, nstart=None, weight='weight',
                       dangling=None):
    # Create a copy in (right) stochastic form
    W = nx.stochastic_graph(D, weight=weight)
    N = W.number_of_nodes()

    # Choose fixed starting vector if not given
    x = dict.fromkeys(W, 1.0 / N)
    p = dict.fromkeys(W, 1.0 / N)
    dangling_weights = p
    dangling_nodes = [n for n in W if W.out_degree(n, weight=weight) == 0.0]

    # power iteration: make up to max_iter iterations
    for _ in range(max_iter):
        xlast = x
        x = dict.fromkeys(xlast.keys(), 0)
        weight_to_distribute = sum([(xlast[n] * W[n][nbr]['weight'] * edgetype_scale[W[n][nbr]['type']]) for n in x for nbr in W[n]])
        undistributed_weight = 1 - weight_to_distribute
        for n in x:
            # this matrix multiply looks odd because it is
            # doing a left multiply x^T=xlast^T*W
            for nbr in W[n]:
                x[nbr] += xlast[n] * W[n][nbr][weight] * edgetype_scale[W[n][nbr]['type']]
            x[n] += undistributed_weight * dangling_weights.get(n, 0)
        # check convergence, l1 norm
        err = sum([abs(x[n] - xlast[n]) for n in x])
        if err < N * tol:
            return x
    raise nx.PowerIterationFailedConvergence(max_iter)


def test_same_relation_types():
    g = nx.DiGraph()
    g.add_edges_from([('A', 'B', {'type': 'supplier'}),
                      ('A', 'C', {'type': 'supplier'})])
    r = pagerank_edgetypes(g, alpha=1)
    assert ([k for k, v in sorted(r.iteritems(), key=lambda (k, v): v, reverse=True)] == ['C', 'B', 'A'])


def test_same_relation_types_normalized_node_weights():
    g = nx.DiGraph()
    g.add_edges_from([('A', 'B', {'weight': 1.0, 'type': 'supplier'}),
                      ('A', 'C', {'weight': 0.5, 'type': 'supplier'})])
    r = pagerank_edgetypes(g, alpha=1)
    assert ([k for k, v in sorted(r.iteritems(), key=lambda (k, v): v, reverse=True)] == ['B', 'C', 'A'])


def test_multiple_relation_types():
    g = nx.DiGraph()
    g.add_edges_from([('A', 'B', {'type': 'supplier'}),
                      ('A', 'B', {'type': 'sector'}),
                      ('A', 'C', {'type': 'supplier'})])
    r = pagerank_edgetypes(g, alpha=1)
    assert ([k for k, v in sorted(r.iteritems(), key=lambda (k, v): v, reverse=True)] == ['C', 'B', 'A'])


# Would expect the same order as in test_same_relation_types but it's off as
# B and C are different on the 16th decimal place
def test_multiple_relation_types_circular():
    g = nx.DiGraph()
    g.add_edges_from([('A', 'B', {'type': 'sector'}),
                      ('B', 'A', {'type': 'sector'}),
                      ('A', 'C', {'type': 'sector'}),
                      ('C', 'A', {'type': 'sector'}),
                      ('B', 'C', {'type': 'sector'}),
                      ('C', 'B', {'type': 'sector'}),
                      ('A', 'B', {'type': 'supplier'}),
                      ('A', 'C', {'type': 'supplier'})])
    r = pagerank_edgetypes(g, alpha=1)
    assert ([k for k, v in sorted(r.iteritems(), key=lambda (k, v): v, reverse=True)] == ['B', 'C', 'A'])




test_multiple_relation_types_circular()