package performance;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

public class Graph {
    private Map<String, Queue<String>> graph;

    public Graph(Map<String, Queue<String>> graph) {
        this.graph = graph;
    }

    public void record(String key, String value) {
        graph.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>());
        graph.get(key).add(value);
    }

    public String generateData(String name) {
        String result = name + ":=";
        for(Entry<String,Queue<String>> entry : graph.entrySet()) {

        }
        return null;
    }

}
