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
  end

  def load_graph
    return unless File.exist? @file_name
    File.open(@file_name, 'r').each_line.with_index { |line, index|
      next if line[0]=="#"
      line = line.split("\s").map {|vertex_id| vertex_id.to_i }

      #skip vertex, if it belongs to the other worker
      next if graph_partition_for_vertex(line[0]) != @worker_id
      add_vertex(line)
      # puts index if (index%5000 == 0)
    }
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
  end

  private
  def graph_partition_for_vertex vertex_id
    vertex_id % @total_workers
  end

  def add_vertex input_line
    # Not searching for whether this vertex has been encountered before -
    # find() slows down the loading of large graphs.
    # For now, assuming that input files are ordered, i.e.
    # all rows for one vertex are colocated: 1 1\n 1 1\n 2 1\n 2 5\n
    # vertex = @vertices.find{|vertex| vertex[0] == input_line[0] }
    if(!@vertices.empty? and input_line[0] == @vertices.last[0])
      #add adjacent vertex to the list of adjacent vertices of current vertex
      @vertices.last[1] << input_line[1]
    else
      #vertex[1] - an array of adjacent vertices that current vertex points to
      vertex = [input_line[0], [input_line[1]]]
      @vertices << vertex
    end
  end
end