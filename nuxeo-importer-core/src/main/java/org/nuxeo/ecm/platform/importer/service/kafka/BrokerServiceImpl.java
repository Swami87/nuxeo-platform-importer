package org.nuxeo.ecm.platform.importer.service.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.nuxeo.ecm.platform.importer.service.kafka.broker.EventBroker;
import org.nuxeo.ecm.platform.importer.service.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.settings.Settings;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by anechaev on 7/27/16.
 * © Andrei Nechaev 2016
 */
public class BrokerServiceImpl implements BrokerService {

    private static Log log = LogFactory.getLog(BrokerServiceImpl.class);
    private EventBroker mBroker;
    private ExecutorService mConsumerExecutor;
    private int mPartition;
    private int mReplication;

    public BrokerServiceImpl(EventBroker broker) {
        this.mBroker = broker;
    }

    @Override
    public void populateConsumers(Properties props) {
        if (mConsumerExecutor == null) {

        }

        int num = mPartition * mReplication;

        mBroker.createTopic(Settings.TASK, mPartition, mReplication);

        mConsumerExecutor = Executors.newFixedThreadPool(num);

        for (int i = 0; i < num; i++) {
            Consumer<String, String> consumer = new Consumer<>(props);
            consumer.subscribe(Collections.singletonList(Settings.TASK));

            Runnable task = () -> {
                ConsumerRecords<String, String> crs = consumer.poll(2000);

                while (crs.iterator().hasNext()) {
                    ConsumerRecord<String, String> record = crs.iterator().next();
                    log.info(record.key() + ": " + record.value());
                    crs = consumer.poll(2000);
                }
            };

            mConsumerExecutor.execute(task);
        }

        mConsumerExecutor.shutdown();
    }

    @Override
    public void terminateServicePool() {
        if (mConsumerExecutor != null) {
            try {
                mConsumerExecutor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error(e);
            }
        }
    }

    @Override
    public int getPartition() {
        return this.mPartition;
    }

    @Override
    public void setPartition(int partition) {
        this.mPartition = partition;
    }

    @Override
    public int getReplication() {
        return this.mReplication;
    }

    @Override
    public void setReplication(int replication) {
        this.mReplication = replication;
    }
}
