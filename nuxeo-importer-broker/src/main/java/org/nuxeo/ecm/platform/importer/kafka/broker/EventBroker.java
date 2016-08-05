package org.nuxeo.ecm.platform.importer.kafka.broker;


import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;
import org.nuxeo.ecm.platform.importer.kafka.settings.Settings;
import org.nuxeo.ecm.platform.importer.kafka.zk.ZooKeeperStartable;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * (C) Copyright 2006-2016 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Andrei Nechaev
 */

public class EventBroker {
    private static final Log log = LogFactory.getLog(EventBroker.class);

    private final ExecutorService mZKService = Executors.newSingleThreadExecutor();
    private final ExecutorService mInternalService = Executors.newCachedThreadPool();

    private ZooKeeperStartable mZKServer;
    private KafkaServerStartable mKafkaServer;


    public EventBroker(Map<String, String> properties) throws Exception {
        String zkProps = properties.get(Settings.ZOOKEEPER);

        Properties zp = ServiceHelper.loadProperties(zkProps);
        zp.setProperty("dataDir", Files.createTempDirectory("zp-").toAbsolutePath().toString());
        mZKServer = new ZooKeeperStartable(zp);

        String kafkaProps = properties.get(Settings.KAFKA);

        Properties kp = ServiceHelper.loadProperties(kafkaProps);
        kp.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        KafkaConfig kafkaConfig = new KafkaConfig(kp);

        mKafkaServer = new KafkaServerStartable(kafkaConfig);
    }

    public EventBroker(Properties kfProps, Properties zkProps) throws IOException {
        mZKServer = new ZooKeeperStartable(zkProps);
        mKafkaServer = new KafkaServerStartable(new KafkaConfig(kfProps));
    }


    public void start() throws Exception {
        if (mZKServer == null || mKafkaServer == null) {
            throw new Exception("Could not load the Broker");
        }

        mZKService.execute(mZKServer.start());
        mZKService.shutdown();

        mKafkaServer.startup();
        log.info("Broker started");
    }


    public void stop() throws Exception {
        if (mKafkaServer == null) {
            throw new Exception("Could not stop the Broker");
        }

        mKafkaServer.shutdown();
        mKafkaServer.awaitShutdown();

        mInternalService.shutdown();
        mInternalService.awaitTermination(5, TimeUnit.MINUTES);

        mZKServer.stop();
        mZKService.awaitTermination(5, TimeUnit.MINUTES);
    }

    
    public void createTopic(String name, int partition, int replication) {
        mInternalService.execute(() -> {
            try {
                mZKServer.createTopic(name, partition, replication);
            } catch (Exception e) {
                log.error(e);
            }
        });
    }
}
