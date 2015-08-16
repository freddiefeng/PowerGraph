#include <vector>
#include <string>
#include <fstream>
#include <fstream>
#include <chrono>

#include <graphlab.hpp>

// There is no edge data in the pagerank application
typedef graphlab::empty edge_data_type;

typedef graphlab::empty vertex_data_type;

// The graph type is determined by the vertex and edge data types
typedef graphlab::distributed_graph<vertex_data_type, edge_data_type> graph_type;

struct some_writer {
	std::string save_vertex(graph_type::vertex_type v) {
		std::stringstream strm;
		strm << v.id() << "\n";
		return strm.str();
	}
	std::string save_edge(graph_type::edge_type e) {
		std::stringstream strm;
		strm << e.source().id() << "\t" << e.target().id() << "\n";
		return strm.str();
	}
}; // end of some writer

int main(int argc, char** argv) {
	// Initialize control plain using mpi
	graphlab::mpi_tools::init(argc, argv);
	graphlab::distributed_control dc;
	global_logger().set_log_level(LOG_INFO);

	// Parse command line options -----------------------------------------------
	graphlab::command_line_options clopts("PageRank algorithm.");
	std::string graph_dir;
	std::string format = "adj";
	std::string updates_dir;
	std::string exec_type = "synchronous";
	clopts.attach_option("graph", graph_dir,
						"The graph file. Required ");
	clopts.add_positional("graph");

	clopts.attach_option("updates", updates_dir,
						"The edge update file");
	clopts.add_positional("updates");
	
	clopts.attach_option("format", format,
						"The graph file format");
	
	clopts.attach_option("engine", exec_type, 
						"The engine type synchronous or asynchronous");
	
	std::string saveprefix;
	clopts.attach_option("saveprefix", saveprefix,
						"If set, will save the resultant pagerank to a "
						"sequence of files with prefix saveprefix");

	if(!clopts.parse(argc, argv)) {
		dc.cout() << "Error in parsing command line arguments." << std::endl;
		return EXIT_FAILURE;
	}

	if (graph_dir == "") {
		dc.cout() << "Graph not specified. Cannot continue";
		return EXIT_FAILURE;
	}

	if (updates_dir == "") {
		dc.cout() << "Update file not specified. Cannot continue";
		return EXIT_FAILURE;
	}

	// Build the initial graph ----------------------------------------------------------
	graph_type graph(dc, clopts);
	dc.cout() << "Loading graph in format: "<< format << std::endl;
	graph.load_format(graph_dir, format);
	// must call finalize before querying the graph
	graph.finalize();
	dc.cout() << "#vertices: " << graph.num_vertices() << " #edges:" << graph.num_edges() << std::endl;

	auto t_start = std::chrono::high_resolution_clock::now();

	// Update the graph  ----------------------------------------------------------
	std::ifstream infile;
	infile.open(updates_dir.c_str());
	long src, dst;

	while(infile >> src >> dst) {
		if (src < 0)
			src = ~src;
		if (dst < 0)
			dst = ~dst;
		std::cout << "adding " << src << " " << dst << std::endl;
	}

	graphlab::vertex_id_type source, target;
	source = (graphlab::vertex_id_type)1;
	target = (graphlab::vertex_id_type)10;
	graph.add_vertex((graphlab::vertex_id_type)10);
	graph.add_edge(source, target);

	graph.finalize();

	infile.close();

	auto t_end = std::chrono::high_resolution_clock::now();

	std::cout << "Graph update took: "
              << std::chrono::duration<double, std::milli>(t_end-t_start).count()
              << " ms\n";

	// Save the final graph -----------------------------------------------------
	if (saveprefix != "") {
		graph.save(saveprefix, some_writer(),
			false,    // do not gzip
			true,     // save vertices
			true);   // do not save edges
	}
	// Tear-down communication layer and quit -----------------------------------
	graphlab::mpi_tools::finalize();
}