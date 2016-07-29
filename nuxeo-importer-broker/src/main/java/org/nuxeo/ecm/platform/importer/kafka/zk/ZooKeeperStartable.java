package org.nuxeo.ecm.platform.importer.kafka.zk;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

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
    private ZkUtils mUtils;

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

    public ZkUtils getUtils() {
        return mUtils;
    }

    public void setUtils(ZkUtils mUtils) {
        this.mUtils = mUtils;
    }

    public void createTopic(String name, int partition, int replication) throws Exception {
        if (mUtils == null) {
            throw new Exception("ZkUtils is not assigned to " + ZooKeeperStartable.class.getName());
        }

        if (AdminUtils.topicExists(mUtils, name)) return;

        AdminUtils.createTopic(mUtils, name, partition, replication, new Properties(), RackAwareMode.Disabled$.MODULE$);
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
