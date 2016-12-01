### "Think Like a Vertex" on Bloom

Implementation of Pregel model on Bloom. Current prototype do not implement fault tolerance properties of the original system. 

#### How to run

To start a master simply run:
```
ruby pregel/start_master.rb
```

It starts a master process on default address `127.0.0.1:1234`. Provide `ip:port` in order to bind master process to specific port.

To start worker processes run:
```
ruby pregel/start_worker.rb master_address worker_address
```

If multiple workers are spawned in same machine, then care must be taken to provide distinct port to each worker process

To start a user interface:
```
ruby pregel/start_master_stdio.rb
```

Starts an command line interface to interact with master process. Provide master address in form of `ip:port`, no argument if master binds to default `127.0.0.1:1234`. Once CLI is running, graph data can be loaded with `load path_to_graph_file`. Current implementation assumes adjacency list format where each line defines a vertex in graph in following form:
```
vertex_id out_degree [adjacent_vertex_1 adjacent_vertex_2 ...]
```

If input graph is in edge list format, small transform script is provided. Simply run
```
ruby pregel/graph_converter.rb input_file output_file
```
