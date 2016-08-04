package org.nuxeo.ecm.platform.importer.kafka.zk;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.nuxeo.ecm.platform.importer.kafka.settings.Settings;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by anechaev on 7/25/16.
 * © Andre Nechaev 2016
 */
public class ZooKeeperStartable {
    private static final Log log = LogFactory.getLog(ZooKeeperStartable.class);

    private final ServerConfig mConfiguration = new ServerConfig();
    private ExecutorService mServiceExecutor = Executors.newSingleThreadExecutor();

    private ZooKeeperServerMain mZooKeeperServer;

    public ZooKeeperStartable(Properties properties) throws IOException {
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(properties);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        mZooKeeperServer = new ZooKeeperServerMain();
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

    public Runnable start() throws Exception {
        if (mZooKeeperServer == null) {
            throw new Exception("ZooKeeper failed");
        }

        return () -> {
            log.info("Zookeeper started");
            try {
                mZooKeeperServer.runFromConfig(mConfiguration);
            } catch(Exception e) {
                log.error(e);
            }
        };
    }

    public void stop() throws InterruptedException {
        try {
            // Using protected method of ZooKeeperServerMain class via reflection
            Method shutdown = ZooKeeperServerMain.class.getDeclaredMethod("shutdown");
            shutdown.setAccessible(true);
            shutdown.invoke(mZooKeeperServer);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            log.error(e);
        }

        mServiceExecutor.shutdown();
        mServiceExecutor.awaitTermination(60, TimeUnit.MINUTES);
    }
}
