/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     tiry
 */
package org.nuxeo.ecm.platfrom.importer.kafka.features;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.SimpleFeature;

import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Simple test feature used to deploy a Kafka/Zookeeper infrastructure
 *
 * @since 8.4
 */
public abstract class AbstractKafkaFeature extends SimpleFeature {

    private static final String ZK_HOST = "127.0.0.1";

    private KafkaServer kafkaServer;

    private ZkClient zkClient;

    private EmbeddedZookeeper zkServer;

    private static final Log log = LogFactory.getLog(AbstractKafkaFeature.class);


    @Override
    public void beforeRun(FeaturesRunner runner) throws Exception {

        log.debug("**** Starting Kafka test environment");

        // setup ZooKeeper
        zkServer = new EmbeddedZookeeper();

        String zkConnect = ZK_HOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        Properties brokerProps = ServiceHelper.loadProperties("kafka.props");
        brokerProps.put("zookeeper.connect", zkConnect);
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());

        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        kafkaServer.startup();

        if (zkUtils.getAllBrokersInCluster().size() == 0) {
            throw new RuntimeException("Cluster not started");
        }

        assertEquals(1, zkUtils.getAllBrokersInCluster().size());

        int topicPartition = 4;
        int topicReplicationFactor = 1;

        List<KafkaServer> servers = Collections.singletonList(kafkaServer);

        propagateTopics(zkUtils, servers, topicReplicationFactor, topicPartition);

        log.debug("**** Kafka test environment Started");
    }

    public abstract void propagateTopics(ZkUtils utils, List<KafkaServer> servers, Integer replications, Integer partitions);

    @Override
    public void afterRun(FeaturesRunner runner) throws Exception {
        log.debug("**** Shutting down Kafka test environment");
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
        log.debug("**** Kafka test environment Stopped");
    }

}
