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

package org.apache.tinkerpop.gremlin.process.traversal.traverser;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyPath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class DefaultTraverserGenerator implements TraverserGenerator {

    private static final DefaultTraverserGenerator INSTANCE = new DefaultTraverserGenerator();

    private DefaultTraverserGenerator() {
    }

    @Override
    public <S> Traverser.Admin<S> generate(final S start, final Step<S, ?> startStep, final long initialBulk) {
        final Set<TraverserRequirement> requirements = TraversalHelper.getRootTraversal(startStep.getTraversal()).getTraverserRequirements();
        if (requirements.contains(TraverserRequirement.PATH))
            return new DefaultTraverser<>(start, startStep, requirements.contains(TraverserRequirement.ONE_BULK) ? Long.MIN_VALUE : initialBulk, ImmutablePath.make().extend(start, startStep.getLabels()), false);
        else if (requirements.contains(TraverserRequirement.LABELED_PATH))
            return new DefaultTraverser<>(start, startStep, requirements.contains(TraverserRequirement.ONE_BULK) ? Long.MIN_VALUE : initialBulk, ImmutablePath.make().extend(start, startStep.getLabels()), true);
        else
            return new DefaultTraverser<>(start, startStep, requirements.contains(TraverserRequirement.ONE_BULK) ? Long.MIN_VALUE : initialBulk, EmptyPath.instance(), true);
    }

    @Override
    public Set<TraverserRequirement> getProvidedRequirements() {
        return Collections.emptySet();
    }

    public static DefaultTraverserGenerator instance() {
        return INSTANCE;
    }
}