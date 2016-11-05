# require 'rubygems'
# require 'debugger'

require_relative 'graph_loader.rb'

describe DistributedGraphLoader do
  it "should load a partition of graph per worker_id (subset of vertices)" do
    graph_loader = DistributedGraphLoader.new 'sample_graph.txt', 0, 3
    graph_loader.load_graph
    expect(graph_loader.vertices).to eq([[3, [1, 2, 3]], [6, [1, 2, 5]]])

    graph_loader = DistributedGraphLoader.new 'sample_graph.txt', 1, 3
    graph_loader.load_graph
    expect(graph_loader.vertices).to eq([[1, [1, 2, 3]], [4, [3, 5, 6]]])

    graph_loader = DistributedGraphLoader.new 'sample_graph.txt', 2, 3
    graph_loader.load_graph
    expect(graph_loader.vertices).to eq([[2, [1, 2, 3]], [5, [2, 3, 6]]])
    expect(graph_loader.vertices.size).to eq 2
  end

  # it "should load citations graph" do
  #   graph_loader = DistributedGraphLoader.new 'cit-HepTh.txt', 0, 1
  #   graph_loader.load_graph
  #   expect(graph_loader.vertices.size).to eq 27770
  # end
end