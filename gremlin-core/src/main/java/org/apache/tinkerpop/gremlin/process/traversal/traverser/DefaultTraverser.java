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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyPath;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class DefaultTraverser<T> implements Traverser.Admin<T> {

    private T t;
    private long bulk = 1l;
    private String stepId;
    private short loops = 0;
    private Set<String> tags;
    private Object sack;
    private Path path;
    private transient TraversalSideEffects sideEffects;
    ///
    private boolean onlyLabeledPaths; // we need to get rid of this


    /**
     * A no-args constructor  is necessary for Kryo serialization.
     */
    private DefaultTraverser() {

    }

    public DefaultTraverser(final T t, final Step<T, ?> step, final long initialBulk, final Path path, boolean onlyLabeledPaths) {
        this.t = t;
        this.stepId = step.getId();
        this.bulk = initialBulk;
        this.path = path instanceof EmptyPath ? null : path;
        this.sideEffects = step.getTraversal().getSideEffects();
        this.onlyLabeledPaths = onlyLabeledPaths;
        if (null != this.sideEffects.getSackInitialValue())
            this.sack = this.sideEffects.getSackInitialValue().get();
    }


    @Override
    public void addLabels(final Set<String> labels) {  // we need to get rid of this too
        if (null != this.path) {
            if (this.onlyLabeledPaths) {
                if (!labels.isEmpty())
                    this.path = this.path.size() == 0 || !this.path.get(this.path.size() - 1).equals(this.t) ?
                            this.path.extend(this.t, labels) :
                            this.path.extend(labels);
            } else
                this.path = this.path.extend(labels);
        }
    }

    @Override
    public void set(final T t) {
        this.t = t;
    }

    @Override
    public void incrLoops(final String stepLabel) {
        this.loops++;
    }

    @Override
    public void resetLoops() {
        this.loops = 0;
    }

    @Override
    public String getStepId() {
        return this.stepId;
    }

    @Override
    public void setStepId(final String stepId) {
        this.stepId = stepId;
    }

    @Override
    public void setBulk(final long count) {
        this.bulk = count;
    }

    @Override
    public Admin<T> detach() {
        this.t = ReferenceFactory.detach(this.t);
        if (null != this.path)
            this.path = ReferenceFactory.detach(this.path);
        return this;
    }

    @Override
    public T attach(final Function<Attachable<T>, T> method) {
        // you do not want to attach a path because it will reference graph objects not at the current vertex
        if (this.t instanceof Attachable && !(((Attachable) this.t).get() instanceof Path))
            this.t = ((Attachable<T>) this.t).attach(method);
        return this.t;
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {
        this.sideEffects = sideEffects;
    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return this.sideEffects;
    }

    public Set<String> getTags() {
        if (null == this.tags) this.tags = new HashSet<>();
        return this.tags;
    }

    @Override
    public <R> Admin<R> split(final R r, final Step<T, R> step) {
        final DefaultTraverser<R> clone = (DefaultTraverser<R>) this.clone();
        clone.t = r;
        if (null != this.path) {
            if (this.onlyLabeledPaths) {
                if (!step.getLabels().isEmpty())
                    clone.path = clone.path.extend(r, step.getLabels());
            } else
                clone.path = clone.path.extend(r, step.getLabels());
        }
        return clone;
    }

    @Override
    public Admin<T> split() {
        return (DefaultTraverser.Admin<T>) this.clone();
    }

    @Override
    public void merge(final Traverser.Admin<?> other) {
        if (-1 != this.bulk)
            this.bulk = this.bulk + other.bulk();
        if (!other.getTags().isEmpty()) {
            if (this.tags == null) this.tags = new HashSet<>();
            this.tags.addAll(other.getTags());
        }
        if (null != this.sack && null != this.sideEffects.getSackMerger())
            this.sack = this.sideEffects.getSackMerger().apply(this.sack, other.sack());
    }

    @Override
    public T get() {
        return this.t;
    }

    @Override
    public <S> S sack() {
        return (S) this.sack;
    }

    @Override
    public <S> void sack(final S object) {
        this.sack = object;
    }

    @Override
    public Path path() {
        return null == this.path ? EmptyPath.instance() : this.path;
    }

    @Override
    public int loops() {
        return this.loops;
    }

    @Override
    public long bulk() {
        return this.bulk == -1 ? 1 : this.bulk;
    }

    @Override
    public Traverser<T> clone() {
        try {
            final DefaultTraverser<T> clone = (DefaultTraverser<T>) super.clone();
            if (null != this.tags)
                clone.tags = new HashSet<>(this.tags);
            if (null != this.path)
                clone.path = this.path.clone();
            if (null != this.sack)
                clone.sack = null == clone.sideEffects.getSackSplitter() ? this.sack : clone.sideEffects.getSackSplitter().apply(this.sack);
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    ///////////

    @Override
    public int hashCode() {
        return this.t.hashCode() + this.stepId.hashCode() + this.loops + (null == this.path ? 0 : this.path.hashCode());
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof DefaultTraverser &&
                this.t.equals(((DefaultTraverser) object).t) &&
                this.stepId.equals(((DefaultTraverser) object).stepId) &&
                this.loops == ((DefaultTraverser) object).loops &&
                (null == this.sack || (null != this.sideEffects && null != this.sideEffects.getSackMerger())) && // hmmm... serialization in OLAP destroys the transient sideEffects
                (null == this.path || this.path.equals(((DefaultTraverser) object).path));
    }

    @Override
    public String toString() {
        return this.t.toString();
    }
}
