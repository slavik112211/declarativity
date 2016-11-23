# require 'rubygems'
# require 'debugger'
require_relative 'graph_loader.rb'

describe DistributedGraphLoader do
  # Vertex is stored in the following format:
  # [vertex_id, vertex_value, total_adjacent_vertices, [vertices_it_points_to]]
  # where vertex_value is set to 1/total_vertex_number (init for PageRank)
  it "should load a partition of graph per worker_id (subset of vertices)" do
    graph_loader = DistributedGraphLoader.new 'datasets/sample_graph.txt', 0, 3
    graph_loader.load_graph
    expect(graph_loader.vertices).to eq([[3, 0.16666666666666666, 3, [1, 2, 3]], [6, 0.16666666666666666, 3, [1, 2, 5]]])

    graph_loader = DistributedGraphLoader.new 'datasets/sample_graph.txt', 1, 3
    graph_loader.load_graph
    expect(graph_loader.vertices).to eq([[1, 0.16666666666666666, 3, [1, 2, 3]], [4, 0.16666666666666666, 3, [3, 5, 6]]])

    graph_loader = DistributedGraphLoader.new 'datasets/sample_graph.txt', 2, 3
    graph_loader.load_graph
    expect(graph_loader.vertices).to eq([[2, 0.16666666666666666, 3, [1, 2, 3]], [5, 0.16666666666666666, 3, [2, 3, 6]]])
    expect(graph_loader.vertices.size).to eq 2
  end

  it "should include vertices with no out-edges" do
    graph_loader = DistributedGraphLoader.new 'datasets/wikipedia_sample', 0, 1
    graph_loader.load_graph
    # expect(graph_loader.vertices.size).to eq 11
    expect(graph_loader.vertices).to eq(
      [[2, 0.09090909090909091, 1, [3]],
       [3, 0.09090909090909091, 1, [2]],
       [4, 0.09090909090909091, 2, [1, 2]],
       [5, 0.09090909090909091, 3, [2, 4, 6]],
       [6, 0.09090909090909091, 2, [2, 5]],
       [7, 0.09090909090909091, 2, [2, 5]],
       [8, 0.09090909090909091, 2, [2, 5]],
       [9, 0.09090909090909091, 2, [2, 5]],
       [10,0.09090909090909091, 1, [5]],
       [11,0.09090909090909091, 1, [5]],
       [1, 0.09090909090909091, 0, []]]
    )
  end

  it "should load citations graph" do
    graph_loader = DistributedGraphLoader.new 'datasets/cit-HepTh.txt', 0, 1
    graph_loader.load_graph
    graph_loader.graph_stats
    expect(graph_loader.vertices_from.size).to eq 25059
    expect(graph_loader.vertices_to.size)  .to eq 23180
    expect(graph_loader.vertices_all.size) .to eq 27770
    expect(graph_loader.vertices.size)     .to eq 27770
  end

 end

 describe AdjacencyListGraphLoader do
   # this part is about the adjacency graph loader
  it "should load a partition of graph per worker_id (subset of vertices)" do
    graph_loader = AdjacencyListGraphLoader.new 'datasets/sample_graph_adjacency.txt', 0, 3
    graph_loader.load_graph
    expect(graph_loader.vertices).to eq([[3, 0.16666666666666666, 3, [1, 2, 3]], [6, 0.16666666666666666, 3, [1, 2, 5]]])
    expect(graph_loader.vertices.size).to eq 2

    graph_loader = AdjacencyListGraphLoader.new 'datasets/sample_graph_adjacency.txt', 1, 3
    graph_loader.load_graph
    expect(graph_loader.vertices).to eq([[1, 0.16666666666666666, 3, [1, 2, 3]], [4, 0.16666666666666666, 3, [3, 5, 6]]])
    expect(graph_loader.vertices.size).to eq 2

    graph_loader = AdjacencyListGraphLoader.new 'datasets/sample_graph_adjacency.txt', 2, 3
    graph_loader.load_graph
    expect(graph_loader.vertices).to eq([[2, 0.16666666666666666, 3, [1, 2, 3]], [5, 0.16666666666666666, 3, [2, 3, 6]]])
    expect(graph_loader.vertices.size).to eq 2
  end

end