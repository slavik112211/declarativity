# require 'rubygems'
# require 'debugger'

# Graph input file format is as follows:
#
# [node1] [node2]
# [node1] [node3]
# [node2] [node3]
class DistributedGraphLoader
  attr_reader :vertices

  def initialize file_name, worker_id, total_workers
    @file_name=file_name
    @worker_id=worker_id
    @total_workers=total_workers
  end

  def load_graph
    @vertices = Array.new
    @file_name ||= "graph.txt"

    File.open(@file_name, 'r').each_line.with_index { |line, index|
      next if line[0]=="#"
      line = line.split("\s").map {|vertex_id| vertex_id.to_i }

      #skip vertex, if it belongs to the other worker
      next if graph_partition_for_vertex(line[0]) != @worker_id
      add_vertex(line)
      # puts index if (index%5000 == 0)
    }
  end

  private
  def graph_partition_for_vertex vertex_id
    vertex_id % @total_workers
  end

  def add_vertex input_line
    vertex = @vertices.find{|vertex| vertex[0] == input_line[0] }
    if(vertex)
      #add adjacent vertex to the list of adjacent vertices of current vertex
      vertex[1] << input_line[1]
    else
      #vertex[1] - an array of adjacent vertices that current vertex points to
      vertex = [input_line[0], [input_line[1]]]
      @vertices << vertex
    end
  end
end