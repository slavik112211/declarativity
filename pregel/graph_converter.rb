
header = "# Vertex ID | Out Degree | [Adjacent vertex IDs]"

if (ARGV.length < 2) 
	puts "Both source and target files must be specified"
else 
	source_graph_file = ARGV[0]
	target_graph_file = ARGV[1]

	adjaceny_list = Hash.new

	File.open(source_graph_file, "r").each_line { |edge|
		next if edge.start_with?('#')
		source_vertex = edge.split[0]
		target_vertex = edge.split[1]

		unless adjaceny_list.has_key?(source_vertex)
			adjaceny_list[source_vertex] = [target_vertex] 
		else	
			adjaceny_list[source_vertex] << target_vertex
		end

		# to make sure that vertices with no outgoing edges also appear
		unless adjaceny_list.has_key?(target_vertex)
			adjaceny_list[target_vertex] = []
		end
	}

	File.open(target_graph_file, "w") do |f|
		f.puts header
  		adjaceny_list.each{ |vertex, adjaceny|
			line = vertex + " " + adjaceny.size.to_s
			adjaceny.each{ | neighbour | line << " " + neighbour}
			f.puts line
		}
	end
end
