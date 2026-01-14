from __future__ import annotations


def task_write_graph_to_neo4j(graph_file: str, **_context) -> int:
	from helpers.neo4j.adapter import load_graph_from_file, write_graph_to_db

	graph = load_graph_from_file(graph_file)
	write_graph_to_db(graph)
	return 0
