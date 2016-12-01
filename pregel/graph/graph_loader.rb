# require 'rubygems'
# require 'debugger'
require 'set'

# Graph input file format is as follows:
#
# [node1] [node2]
# [node1] [node3]
# [node2] [node3]
class DistributedGraphLoader
  attr_accessor :vertices, :file_name, :worker_id, :total_workers
  attr_reader :vertices_from, :vertices_to, :vertices_all, :graph_size
  def initialize(file_name="graph.txt", worker_id=0, total_workers=1)
    @vertices = Array.new
    @file_name=file_name
    @worker_id=worker_id
    @total_workers=total_workers
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
    load_dead_end_vertices
  end

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
    @graph_size = @vertices_all.size
  end

  def load_dead_end_vertices
    dead_end_vertices = @vertices_to - @vertices_from
    dead_end_vertices.each {|vertex_id|
      @vertices << [vertex_id, :regular, 1.to_f/@vertices_all.size, 0, []]
    }
  end

  def graph_partition_for_vertex vertex_id
    vertex_id % @total_workers
  end

  private
  # 1. Not searching for whether this vertex has been encountered before -
  #    find() slows down the loading of large graphs.
  #    For now, assuming that input files are ordered, i.e.
  #    all rows for one vertex are colocated: 1 1\n 1 1\n 2 1\n 2 5\n
  #    vertex = @vertices.find{|vertex| vertex[0] == input_line[0] }
  # 2. Vertex is stored in the following format:
  #    [vertex_id, vertex_type, vertex_value, total_adjacent_vertices, [vertices_it_points_to]]
  #    where vertex_value is set to 1/total_vertex_number (init for PageRank)
  #    vertex_type is :regular, :master or :ghost
  def add_vertex input_line
    if(!@vertices.empty? and input_line[0] == @vertices.last[0])
      #add adjacent vertex to the list of adjacent vertices of current vertex
      @vertices.last[4] << input_line[1]
    else
      calc_total_adjacent_vertices(@vertices.last) if (!@vertices.empty?)
      #vertex[2] - an array of adjacent vertices that current vertex points to
      vertex = [input_line[0], :regular, 1.to_f/@vertices_all.size, 1, [input_line[1]]]
      @vertices << vertex
    end
  end

  def calc_total_adjacent_vertices vertex
    vertex[3] = vertex[4].size
  end
end




# Graph input file format is as follows:
#
# [node1] [# of neighbours] [neighbour1, neighbour2, ...]
# [node2] [# of neighbours] [neighbour1, neighbour2, ...]
class AdjacencyListGraphLoader
  attr_accessor :vertices, :file_name, :worker_id, :total_workers, :lalp_threshold
  attr_reader :graph_size, :master_vertex_count, :regular_vertex_count
  def initialize(file_name="graph.txt", worker_id=0, total_workers=1, lalp_threshold = 3)
    @vertices = Array.new
    @file_name=file_name
    @worker_id=worker_id
    @total_workers=total_workers
    @lalp_threshold=lalp_threshold
    # total number of master vertices, number of ghost vertices will be @master_vertex_count * (@total_workers - 1)
    @master_vertex_count=0
    # total number of regular vertices that are assigne to this partition
    @regular_vertex_count=0
  end

  def load_graph
    graph_stats
    return unless File.exist? @file_name
    File.open(@file_name, 'r').each_line.with_index { |line, index|
      next if line[0]=="#"
      line = line.split("\s").map {|entry| entry.to_i }

      # first check whether lalp is applied to this vertex. If so every worker will need to partition some part
      if line[1] > @lalp_threshold
        add_lalp_vertex(line)
      # else, just invoke regular vertex loading scheme, only the vertices hashed to this partition      
      elsif graph_partition_for_vertex(line[0]) == @worker_id 
        add_vertex(line)
      #skip vertex, if it belongs to the other worker
      else 
        next
      end
      # puts index if (index%5000 == 0)
    }
  end

  def graph_stats
    return unless File.exist? @file_name
    @graph_size  = 0
    File.open(@file_name, 'r').each_line.with_index { |line, index|
      next if line[0]=="#"
      @graph_size += 1
    }
  end

  def graph_partition_for_vertex vertex_id
    vertex_id % @total_workers
  end

  private
  # Vertex is stored in the following format:
  # [vertex_id, vertex_value, total_adjacent_vertices, [vertices_it_points_to]]
  # where vertex_value is set to 1/total_vertex_number (init for PageRank)
  def add_vertex input_line
    @regular_vertex_count += 1
    vertex = [input_line[0], :regular, 1.to_f/@graph_size, input_line[1], input_line[2..input_line.length]]
    @vertices << vertex
  end

  # Vertex is stored in the following format:
  # [vertex_id, vertex_value, total_adjacent_vertices, [vertices_it_points_to]]
  # where vertex_value is set to 1/total_vertex_number (init for PageRank) and vertices_it_points_to include vertices only in this specific partition
  def add_lalp_vertex input_line
    # first extract the outgoing edges that are assigned to this partition
    local_neighbours = Array.new
    input_line[2..input_line.length].each{ |adjacent_vertex|
      # skip vertices that are not assigned to this machine
      next if graph_partition_for_vertex(adjacent_vertex) != @worker_id
      local_neighbours << adjacent_vertex
    }

    # now create vertex entry with only local neighbours
    vertex_type = graph_partition_for_vertex(input_line[0]) == @worker_id ? :master : :ghost
    # increase count if vertex is a master
    @master_vertex_count += 1 if vertex_type == :master
    # out degree of LALP vertices are total outdegree across all workers, required for correct PageRank
    vertex = [input_line[0], vertex_type, 1.to_f/@graph_size, input_line[1], local_neighbours]
    @vertices << vertex
  end
end