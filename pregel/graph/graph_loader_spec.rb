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
    expect(graph_loader.vertices).to eq([[3, 1, 3, [1, 2, 3]], [6, 1, 3, [1, 2, 5]]])

    graph_loader = DistributedGraphLoader.new 'datasets/sample_graph.txt', 1, 3
    graph_loader.load_graph
    expect(graph_loader.vertices).to eq([[1, 1, 3, [1, 2, 3]], [4, 1, 3, [3, 5, 6]]])

    graph_loader = DistributedGraphLoader.new 'datasets/sample_graph.txt', 2, 3
    graph_loader.load_graph
    expect(graph_loader.vertices).to eq([[2, 1, 3, [1, 2, 3]], [5, 1, 3, [2, 3, 6]]])
    expect(graph_loader.vertices.size).to eq 2
  end

  it "should load citations graph" do
    graph_loader = DistributedGraphLoader.new 'datasets/cit-HepTh.txt', 0, 1
    graph_loader.load_graph
    graph_loader.graph_stats
    expect(graph_loader.vertices_from.size).to eq 25059
    expect(graph_loader.vertices_to.size)  .to eq 23180
    expect(graph_loader.vertices_all.size) .to eq 27770
    expect(graph_loader.vertices.size)     .to eq 25059
  end
end