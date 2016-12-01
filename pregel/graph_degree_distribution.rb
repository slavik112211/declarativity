# given a graph file in adjacency list format (produced by graph_converter.rb) outputs out_degree distribution
header = " Out Degree | # of Vertices"

class DegreeDistribution
	def initialize(source_graph_file, target_graph_file = :console) 
		@source_graph_file = source_graph_file
		@target_graph_file = target_graph_file
	end

	def process
		degree_distribution = Hash.new

		File.open(@source_graph_file, "r").each_line { |line|
			next if line.start_with?('#')
			source_vertex = line.split[0]
			out_degree = line.split[1].to_i

			unless degree_distribution.has_key?(out_degree)
				degree_distribution[out_degree] = 1
			else
				degree_distribution[out_degree] += 1
			end
		}

		file = @target_graph_file == :console ? nil : File.open(@target_graph_file, "w")

		degree_distribution.keys.sort.each { |key|
			vertex_count = degree_distribution[key]
			line = key.to_s + " " + vertex_count.to_s
			if @target_graph_file == :console
				puts line
			else
				file.puts line
			end
		}
	end
end


if (ARGV.length  < 1) 
	puts "Both source and target files must be specified"
elsif (ARGV.length == 1)
	puts "Degree distribution will be printed to console"
	degree_distribution = DegreeDistribution.new(ARGV[0])
	degree_distribution.process
else 
	puts "Degree distribution will be printed to #{ARGV[1]}"
	degree_distribution = DegreeDistribution.new(ARGV[0], ARGV[1])
	degree_distribution.process
end

