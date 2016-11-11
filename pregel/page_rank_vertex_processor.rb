class PageRankVertexProcessor
  attr_accessor :graph_loader
  def compute vertex
    messages = []
    if(!vertex.messages_inbox.nil? and !vertex.messages_inbox.empty?)
      new_vertex_value=0
      vertex.messages_inbox.each {|message|
        new_vertex_value+=message[1]
      }
      vertex.value = 0.15/@graph_loader.vertices_all.size + 0.85*new_vertex_value
      # not considering the random jump factor, for simplicity when testing
      # vertex.value = new_vertex_value
    end

    vertex.vertices_to.each { |adjacent_vertex|
      adjacent_vertex_worker_id = @graph_loader.graph_partition_for_vertex(adjacent_vertex)
      messages << [adjacent_vertex_worker_id, vertex.id, adjacent_vertex, 
        vertex.value.to_f / vertex.total_adjacent_vertices, false, false]
    }
    messages
  end
end