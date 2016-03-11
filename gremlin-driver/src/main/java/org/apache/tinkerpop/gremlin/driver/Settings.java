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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class Settings {

    public int port = 8182;

    public List<String> hosts = new ArrayList<>();

    public SerializerSettings serializer = new SerializerSettings();

    public ConnectionPoolSettings connectionPool = new ConnectionPoolSettings();

    public int nioPoolSize = Runtime.getRuntime().availableProcessors();

    public int workerPoolSize = Runtime.getRuntime().availableProcessors() * 2;

    public String username = null;

    public String password = null;

    public String jaasEntry = null;

    public String protocol = null;

    /**
     * Read configuration from a file into a new {@link Settings} object.
     *
     * @param stream an input stream containing a Gremlin Server YAML configuration
     */
    public static Settings read(final InputStream stream) {
        Objects.requireNonNull(stream);

        final Constructor constructor = new Constructor(Settings.class);
        final TypeDescription settingsDescription = new TypeDescription(Settings.class);
        settingsDescription.putListPropertyType("hosts", String.class);
        settingsDescription.putListPropertyType("serializers", SerializerSettings.class);
        constructor.addTypeDescription(settingsDescription);

        final Yaml yaml = new Yaml(constructor);
        return yaml.loadAs(stream, Settings.class);
    }

    /**
     * Read configuration from a file into a new {@link Settings} object.
     */
    public static Settings from(final Configuration conf) {
        final Settings settings = new Settings();

        if (conf.containsKey("port"))
            settings.port = conf.getInt("port");

        if (conf.containsKey("nioPoolSize"))
            settings.nioPoolSize = conf.getInt("nioPoolSize");

        if (conf.containsKey("workerPoolSize"))
            settings.workerPoolSize = conf.getInt("workerPoolSize");

        if (conf.containsKey("username"))
            settings.username = conf.getString("username");

        if (conf.containsKey("password"))
            settings.password = conf.getString("password");

        if (conf.containsKey("jaasEntry"))
            settings.jaasEntry = conf.getString("jaasEntry");

        if (conf.containsKey("protocol"))
            settings.protocol = conf.getString("protocol");

        if (conf.containsKey("hosts"))
            settings.hosts = conf.getList("hosts").stream().map(Object::toString).collect(Collectors.toList());

        if (conf.containsKey("serializer.className")) {
            final SerializerSettings serializerSettings = new SerializerSettings();
            final Configuration serializerConf = conf.subset("serializer");

            if (serializerConf.containsKey("className"))
                serializerSettings.className = serializerConf.getString("className");

            final Configuration serializerConfigConf = conf.subset("serializer.config");
            if (IteratorUtils.count(serializerConfigConf.getKeys()) > 0) {
                final Map<String,Object> m = new HashMap<>();
                serializerConfigConf.getKeys().forEachRemaining(name -> {
                    m.put(name, serializerConfigConf.getProperty(name));
                });
                serializerSettings.config = m;
            }
            settings.serializer = serializerSettings;
        }

        final Configuration connectionPoolConf = conf.subset("connectionPool");
        if (IteratorUtils.count(connectionPoolConf.getKeys()) > 0) {
            final ConnectionPoolSettings cpSettings = new ConnectionPoolSettings();

            if (connectionPoolConf.containsKey("channelizer"))
                cpSettings.channelizer = connectionPoolConf.getString("channelizer");

            if (connectionPoolConf.containsKey("enableSsl"))
                cpSettings.enableSsl = connectionPoolConf.getBoolean("enableSsl");

            if (connectionPoolConf.containsKey("trustCertChainFile"))
                cpSettings.trustCertChainFile = connectionPoolConf.getString("trustCertChainFile");

            if (connectionPoolConf.containsKey("minSize"))
                cpSettings.minSize = connectionPoolConf.getInt("minSize");

            if (connectionPoolConf.containsKey("maxSize"))
                cpSettings.maxSize = connectionPoolConf.getInt("maxSize");

            if (connectionPoolConf.containsKey("minSimultaneousUsagePerConnection"))
                cpSettings.minSimultaneousUsagePerConnection = connectionPoolConf.getInt("minSimultaneousUsagePerConnection");

            if (connectionPoolConf.containsKey("maxSimultaneousUsagePerConnection"))
                cpSettings.maxSimultaneousUsagePerConnection = connectionPoolConf.getInt("maxSimultaneousUsagePerConnection");

            if (connectionPoolConf.containsKey("maxInProcessPerConnection"))
                cpSettings.maxInProcessPerConnection = connectionPoolConf.getInt("maxInProcessPerConnection");

            if (connectionPoolConf.containsKey("minInProcessPerConnection"))
                cpSettings.minInProcessPerConnection = connectionPoolConf.getInt("minInProcessPerConnection");

            if (connectionPoolConf.containsKey("maxWaitForConnection"))
                cpSettings.maxWaitForConnection = connectionPoolConf.getInt("maxWaitForConnection");

            if (connectionPoolConf.containsKey("maxContentLength"))
                cpSettings.maxContentLength = connectionPoolConf.getInt("maxContentLength");

            if (connectionPoolConf.containsKey("reconnectInterval"))
                cpSettings.reconnectInterval = connectionPoolConf.getInt("reconnectInterval");

            if (connectionPoolConf.containsKey("reconnectInitialDelay"))
                cpSettings.reconnectInitialDelay = connectionPoolConf.getInt("reconnectInitialDelay");

            if (connectionPoolConf.containsKey("resultIterationBatchSize"))
                cpSettings.resultIterationBatchSize = connectionPoolConf.getInt("resultIterationBatchSize");


            settings.connectionPool = cpSettings;
        }

        return settings;
    }

    static class ConnectionPoolSettings {
        public boolean enableSsl = false;
        public String trustCertChainFile = null;
        public int minSize = ConnectionPool.MIN_POOL_SIZE;
        public int maxSize = ConnectionPool.MAX_POOL_SIZE;
        public int minSimultaneousUsagePerConnection = ConnectionPool.MIN_SIMULTANEOUS_USAGE_PER_CONNECTION;
        public int maxSimultaneousUsagePerConnection = ConnectionPool.MAX_SIMULTANEOUS_USAGE_PER_CONNECTION;
        public int maxInProcessPerConnection = Connection.MAX_IN_PROCESS;
        public int minInProcessPerConnection = Connection.MIN_IN_PROCESS;
        public int maxWaitForConnection = Connection.MAX_WAIT_FOR_CONNECTION;
        public int maxContentLength = Connection.MAX_CONTENT_LENGTH;
        public int reconnectInterval = Connection.RECONNECT_INTERVAL;
        public int reconnectInitialDelay = Connection.RECONNECT_INITIAL_DELAY;
        public int resultIterationBatchSize = Connection.RESULT_ITERATION_BATCH_SIZE;
        public String channelizer = Channelizer.WebSocketChannelizer.class.getName();

        /**
         * @deprecated as of 3.1.1-incubating, and not replaced as this property was never implemented internally
         * as the way to establish sessions
         */
        @Deprecated
        public String sessionId = null;

        /**
         * @deprecated as of 3.1.1-incubating, and not replaced as this property was never implemented internally
         * as the way to establish sessions
         */
        @Deprecated
        public Optional<String> optionalSessionId() {
            return Optional.ofNullable(sessionId);
        }
    }

    public static class SerializerSettings {
        public String className = GraphSONMessageSerializerV1d0.class.getCanonicalName();
        public Map<String, Object> config = null;

        public MessageSerializer create() throws Exception {
            final Class clazz = Class.forName(className);
            final MessageSerializer serializer = (MessageSerializer) clazz.newInstance();
            Optional.ofNullable(config).ifPresent(c -> serializer.configure(c, null));
            return serializer;
        }
    }
}
