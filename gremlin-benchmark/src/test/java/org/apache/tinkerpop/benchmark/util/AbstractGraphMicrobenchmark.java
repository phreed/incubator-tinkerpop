package org.apache.tinkerpop.benchmark.util;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Created by twilmes on 2/7/16.
 */
@State(Scope.Thread)
public class AbstractGraphMicrobenchmark extends AbstractMicrobenchmark {

    public volatile Graph graph;
    public volatile GraphTraversalSource g;

    @Setup
    public void setup() {
        System.out.println("Declare: " + this.getClass());
        final LoadGraphWith[] loadGraphWiths = this.getClass().getAnnotationsByType(LoadGraphWith.class);
        final LoadGraphWith loadGraphWith = loadGraphWiths.length == 0 ? null : loadGraphWiths[0];
        final LoadGraphWith.GraphData loadGraphWithData = null == loadGraphWith ? null : loadGraphWith.value();
        System.out.println("FOOOOOOOOOOOOOOOOO: " + loadGraphWithData);
        // load graph
        switch(loadGraphWithData) {
            case CLASSIC:
                graph = TinkerFactory.createClassic();
                g = graph.traversal();
                break;
            case MODERN:
                graph = TinkerFactory.createModern();
                break;
            case CREW:
                graph = TinkerFactory.createTheCrew();
                break;
            case GRATEFUL:

                break;
            default:
                break;
        }
    }
}
