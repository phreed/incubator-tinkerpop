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
package org.apache.tinkerpop.gremlin.server.op.traversal;

import io.netty.channel.ChannelHandlerContext;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.Serializer;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Simple {@link OpProcessor} implementation that iterates remotely submitted serialized {@link Traversal} objects.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalOpProcessor extends AbstractOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TraversalOpProcessor.class);
    public static final String OP_PROCESSOR_NAME = "traversal";


    public TraversalOpProcessor() {
        super(true);
    }

    @Override
    public String getName() {
        return OP_PROCESSOR_NAME;
    }

    @Override
    public void close() throws Exception {
        // do nothing = no resources to release
    }

    @Override
    public ThrowingConsumer<Context> select(final Context ctx) throws OpProcessorException {
        final RequestMessage message = ctx.getRequestMessage();
        logger.debug("Selecting processor for RequestMessage {}", message);

        final ThrowingConsumer<Context> op;
        switch (message.getOp()) {
            case Tokens.OPS_TRAVERSE:
                if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
                    final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_TRAVERSE, Tokens.ARGS_GREMLIN);
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                final Optional<Map<String,String>> aliases = message.optionalArgs(Tokens.ARGS_ALIASES);
                if (!aliases.isPresent()) {
                    final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_TRAVERSE, Tokens.ARGS_ALIASES);
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                if (aliases.get().size() != 1) {
                    final String msg = String.format("A message with an [%s] op code requires the [%s] argument to be a Map containing one alias assignment.", Tokens.OPS_TRAVERSE, Tokens.ARGS_ALIASES);
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                final Map.Entry<String,String> kv = aliases.get().entrySet().iterator().next();
                if (!ctx.getGraphManager().getGraphs().containsKey(kv.getValue())) {
                    final String msg = String.format("The graph [%s] for alias [%s] is not configured on the server.", kv.getValue(), kv.getKey());
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                op = this::iterateOp;
                break;
            case Tokens.OPS_INVALID:
                final String msgInvalid = String.format("Message could not be parsed.  Check the format of the request. [%s]", message);
                throw new OpProcessorException(msgInvalid, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).statusMessage(msgInvalid).create());
            default:
                final String msgDefault = String.format("Message with op code [%s] is not recognized.", message.getOp());
                throw new OpProcessorException(msgDefault, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).statusMessage(msgDefault).create());
        }

        return op;
    }

    private void iterateOp(final Context context) throws OpProcessorException {
        final RequestMessage msg = context.getRequestMessage();
        if (logger.isDebugEnabled())
            logger.debug("Traversal request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

        final byte[] serializedTraversal = (byte[]) msg.getArgs().get(Tokens.ARGS_GREMLIN);

        // earlier validation in selection of this op method should free us to cast this without worry
        final Map<String,String> aliases = (Map<String,String>) msg.optionalArgs(Tokens.ARGS_ALIASES).get();

        final Traversal traversal;
        try {
            traversal = (Traversal) Serializer.deserializeObject(serializedTraversal);
        } catch (Exception ex) {
            throw new OpProcessorException("Could not deserialize the Traversal instance",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION).statusMessage(ex.getMessage()).create());
        }

        try {
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final GraphManager graphManager = context.getGraphManager();
            final String graphName = aliases.entrySet().iterator().next().getValue();
            final Graph graph = graphManager.getGraphs().get(graphName);
            final boolean supportsTransactions = graph.features().graph().supportsTransactions();

            traversal.asAdmin().setGraph(graph);

            context.getGremlinExecutor().getExecutorService().submit(() -> {
                try {
                    if (supportsTransactions && graph.tx().isOpen()) graph.tx().rollback();

                    try {
                        // compile the traversal - without it getEndStep() has nothing in it
                        traversal.hasNext();
                        handleIterator(context, new DetachingIterator(traversal.asAdmin().getEndStep()));
                    } catch (TimeoutException ex) {
                        final String errorMessage = String.format("Response iteration exceeded the configured threshold for request [%s] - %s", msg.getRequestId(), ex.getMessage());
                        logger.warn(errorMessage);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(errorMessage).create());
                        if (supportsTransactions && graph.tx().isOpen()) graph.tx().rollback();
                        return;
                    } catch (Exception ex) {
                        logger.warn(String.format("Exception processing a Traversal on iteration for request [%s].", msg.getRequestId()), ex);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
                        if (supportsTransactions && graph.tx().isOpen()) graph.tx().rollback();
                        return;
                    }

                    if (graph.features().graph().supportsTransactions()) graph.tx().commit();
                } catch (Exception ex) {
                    logger.warn(String.format("Exception processing a Traversal on request [%s].", msg.getRequestId()), ex);
                    ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
                    if (graph.features().graph().supportsTransactions() && graph.tx().isOpen()) graph.tx().rollback();
                }
            });

        } catch (Exception ex) {
            throw new OpProcessorException("Could not iterate the Traversal instance",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
        }
    }

    static class DetachingIterator implements Iterator<Traverser> {

        private Iterator<Traverser> inner;

        public DetachingIterator(final Iterator<Traverser> toDetach) {
            inner = toDetach;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Traverser next() {
            return inner.next().asAdmin().detach();
        }
    }
}
