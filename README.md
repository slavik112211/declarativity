### "Think Like a Vertex" in Bloom

Implementation of Google's distributed graph processing model Pregel, using Bloom Bud, a declarative programming framework from UCBerkeley. 

#### How to run

To start a master node (coordinates worker nodes) run:
```
ruby pregel/start_master.rb
```

It starts a master process on default address `127.0.0.1:1234`. Provide `ip:port` in order to bind master process to a specific port.

To start worker processes run:
```
ruby pregel/start_worker.rb master_address worker_address
```

If multiple workers are spawned on the same machine, care must be taken to provide distinct port for each worker process

To start a user IO shell:
```
ruby pregel/start_master_stdio.rb
```

Starts a command line interface to interact with master process. Provide master address in form of `ip:port`, no params needed if master binds to default `127.0.0.1:1234`. Once CLI is running, graph data can be loaded with `load path_to_graph_file`. Current implementation assumes adjacency list format where each line defines a graph vertex in the following format:
```
vertex_id out_degree [adjacent_vertex_1 adjacent_vertex_2 ...]
```

If input graph is in edge list format, a following transformation script can be used:
```
ruby pregel/graph_converter.rb input_file output_file
```
