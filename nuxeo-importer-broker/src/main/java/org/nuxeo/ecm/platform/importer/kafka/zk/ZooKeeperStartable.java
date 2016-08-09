/*
 * (C) Copyright 2006-2016 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * Contributors:
 *     Andrei Nechaev
 */

package org.nuxeo.ecm.platform.importer.kafka.zk;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.nuxeo.ecm.platform.importer.kafka.settings.Settings;

import java.io.IOException;
import java.util.Properties;


public class ZooKeeperStartable implements Runnable {
    private static final Log log = LogFactory.getLog(ZooKeeperStartable.class);

    private final ServerConfig mConfiguration = new ServerConfig();

    private ZKS mZooKeeperServer;

    public ZooKeeperStartable(Properties properties) throws IOException {
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(properties);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        mZooKeeperServer = new ZKS();
        mConfiguration.readFrom(quorumConfiguration);
    }

    public String getHostAddress() {
        return mConfiguration.getClientPortAddress().getHostName()
                + ":"
                + mConfiguration.getClientPortAddress().getPort();
    }

    public void createTopic(String name, int partition, int replication) throws Exception {
        if (mConfiguration == null) {
            throw new Exception("Couldn't configure " + ZooKeeperStartable.class.getName());
        }

        ZkClient client = new ZkClient(
                getHostAddress(),
                Settings.CONNECTION_TIMEOUT,
                Settings.SESSION_TIMEOUT,
                ZKStringSerializer$.MODULE$
        );

        ZkUtils utils = ZkUtils.apply(client, false);

        if (AdminUtils.topicExists(utils, name)) return;

        AdminUtils.createTopic(utils, name, partition, replication, new Properties(), RackAwareMode.Disabled$.MODULE$);
    }

    @Override
    public void run() {
        try {
            mZooKeeperServer.runFromConfig(mConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop() throws InterruptedException {
        mZooKeeperServer.shutdown();
    }
}
