package performance;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Graph {
    private Map<String, List<String>> graph;

    public Graph(Map<String, List<String>> graph) {
        this.graph = graph;
    }

    public void record(String key, String value) {
        graph.computeIfAbsent(key, k -> new LinkedList<>());
        graph.get(key).add(value);
    }

    public String generate() {
        return null;
    }

}
