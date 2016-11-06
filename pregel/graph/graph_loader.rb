# require 'rubygems'
# require 'debugger'
require 'set'

# Graph input file format is as follows:
#
# [node1] [node2]
# [node1] [node3]
# [node2] [node3]
class DistributedGraphLoader
  attr_reader :vertices

  def initialize(file_name="graph.txt", worker_id=0, total_workers=1)
    @vertices = Array.new
    @file_name=file_name
    @worker_id=worker_id
    @total_workers=total_workers
    @init_vertex_value=0
  end

  def load_graph
    graph_stats
    return unless File.exist? @file_name
    File.open(@file_name, 'r').each_line.with_index { |line, index|
      next if line[0]=="#"
      line = line.split("\s").map {|vertex_id| vertex_id.to_i }

      #skip vertex, if it belongs to the other worker
      next if graph_partition_for_vertex(line[0]) != @worker_id
      add_vertex(line)
      # puts index if (index%5000 == 0)
    }
    calc_total_adjacent_vertices(@vertices.last) if (!@vertices.empty?)
  end

  attr_reader :vertices_from, :vertices_to, :vertices_all
  def graph_stats
    return unless File.exist? @file_name
    @vertices_from = Set.new
    @vertices_to   = Set.new
    @vertices_all  = Set.new
    File.open(@file_name, 'r').each_line.with_index { |line, index|
      next if line[0]=="#"
      edge = line.split("\s").map {|vertex_id| vertex_id.to_i }
      @vertices_from.add edge[0]
      @vertices_to.add   edge[1]
      # puts index if (index%5000 == 0)
    }
    @vertices_all.merge(@vertices_from)
    @vertices_all.merge(@vertices_to)
    @init_vertex_value=1.0/@vertices_all.size
  end

  private
  def graph_partition_for_vertex vertex_id
    vertex_id % @total_workers
  end

  # 1. Not searching for whether this vertex has been encountered before -
  #    find() slows down the loading of large graphs.
  #    For now, assuming that input files are ordered, i.e.
  #    all rows for one vertex are colocated: 1 1\n 1 1\n 2 1\n 2 5\n
  #    vertex = @vertices.find{|vertex| vertex[0] == input_line[0] }
  # 2. Vertex is stored in the following format:
  #    [vertex_id, vertex_value, total_adjacent_vertices, [vertices_it_points_to]]
  #    where vertex_value is set to 1/total_vertex_number (init for PageRank)
  def add_vertex input_line
    if(!@vertices.empty? and input_line[0] == @vertices.last[0])
      #add adjacent vertex to the list of adjacent vertices of current vertex
      @vertices.last[3] << input_line[1]
    else
      calc_total_adjacent_vertices(@vertices.last) if (!@vertices.empty?)
      #vertex[2] - an array of adjacent vertices that current vertex points to
      vertex = [input_line[0], @init_vertex_value, 1, [input_line[1]]]
      @vertices << vertex
    end
  end

  def calc_total_adjacent_vertices vertex
    vertex[2] = vertex[3].size
  end
end