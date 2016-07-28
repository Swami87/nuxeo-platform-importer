package org.nuxeo.ecm.platform.importer.service.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
    private ExecutorService consumerExecutor;

    @Override
    public void populateConsumers(Properties props, int num) {
        if (num < 1) {
            num = 1;
        }

        consumerExecutor = Executors.newFixedThreadPool(num);

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

            consumerExecutor.execute(task);
        }
        consumerExecutor.shutdown();
    }

    @Override
    public void terminateServicePool() {
        if (consumerExecutor != null) {
            try {
                consumerExecutor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error(e);
            }
        }
    }
}
