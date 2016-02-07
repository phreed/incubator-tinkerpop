/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process;

import org.apache.tinkerpop.benchmark.util.AbstractGraphMicrobenchmark;
import org.apache.tinkerpop.benchmark.util.AbstractMicrobenchmark;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

/**
 * @author Ted Wilmes
 */
@State(Scope.Thread)
@LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
public class GraphTraversalBenchmark extends AbstractGraphMicrobenchmark {

    @Benchmark
    public Vertex graph_addVertex() {
        return graph.addVertex("foo");
    }

    @Benchmark
    public Vertex g_addV() {
        return g.addV("foo").next();
    }

    @Benchmark
    public List<Vertex> test_g_V() {
        return g.V().toList();
    }

    @Benchmark
    public List<Vertex> test_g_V_outE_inV_outE_inV_outE_inV() {
        return g.V().outE().inV().outE().inV().outE().inV().toList();
    }

    @Benchmark
    public List<Vertex> g_V_out_out_out() {
        return g.V().out().out().out().toList();
    }

    @Benchmark
    public List<Path> g_V_out_out_out_path() {
        return g.V().out().out().out().path().toList();
    }

    @Benchmark
    public List<Vertex> g_V_repeatXoutX_timesX2X() {
        return g.V().repeat(out()).times(2).toList();
    }

    @Benchmark
    public List<Vertex> g_V_repeatXoutX_timesX3X() {
        return g.V().repeat(out()).times(3).toList();
    }

    @Benchmark
    public List<List<Object>> g_V_localXout_out_valuesXnameX_foldX() {
        return g.V().local(out().out().values("name").fold()).toList();
    }

    @Benchmark
    public List<List<Object>> g_V_out_localXout_out_valuesXnameX_foldX() {
        return g.V().out().local(out().out().values("name").fold()).toList();
    }

    @Benchmark
    public List<List<Object>> g_V_out_mapXout_out_valuesXnameX_toListX() {
        return g.V().out().map(v -> g.V(v.get()).out().out().values("name").toList()).toList();
    }

    @Benchmark
    public List<Map<Object, Long>> g_V_label_groupCount() {
        return g.V().label().groupCount().toList();
    }

    @Benchmark
    public List<Object> g_V_match_selectXbX_valuesXnameX() {
        return g.V().match(
                __.as("a").has("name", "Garcia"),
                __.as("a").in("writtenBy").as("b"),
                __.as("a").in("sungBy").as("b")).select("b").values("name").toList();
    }

    @Benchmark
    public List<Edge> g_E_hasLabelXwrittenByX_whereXinV_inEXsungByX_count_isX0XX_subgraphXsgX() {
        return g.E().hasLabel("writtenBy").where(__.inV().inE("sungBy").count().is(0)).subgraph("sg").toList();
    }
}
