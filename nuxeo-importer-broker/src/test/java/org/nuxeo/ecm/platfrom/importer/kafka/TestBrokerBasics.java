package org.nuxeo.ecm.platfrom.importer.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.nuxeo.ecm.platform.importer.kafka.broker.EventBroker;
import org.nuxeo.ecm.platform.importer.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.kafka.producer.Producer;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;
import org.nuxeo.ecm.platform.importer.kafka.settings.Settings;


import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

/**
 * Created by anechaev on 7/28/16.
 * © Andrei Nechaev 2016
 */


public class TestBrokerBasics {

    private static final int COUNT = 100;
    public static final String TOPIC = "test";
    private static ExecutorService es = Executors.newFixedThreadPool(2);

    @Test
    public void testShouldCreateProducer() throws IOException {

        Properties props = ServiceHelper.loadProperties("producer.props");

        Producer<String, String> producer = new Producer<>(props);
        assertNotNull(producer);
    }

    @Test
    public void testShouldCreateConsumer() throws IOException {
        Properties props = ServiceHelper.loadProperties("consumer.props");

        Consumer<String, String> consumer = new Consumer<>(props);
        assertNotNull(consumer);
    }

    @Test
    public void testShouldSendAndReceiveMsgViaBrokerTopic() throws Exception {
        Map<String, String> brokerProps = new HashMap<>();
        brokerProps.put(Settings.KAFKA, "kafka.props");
        brokerProps.put(Settings.ZOOKEEPER, "zk.props");

        EventBroker broker = new EventBroker(brokerProps);


        broker.start();
        broker.createTopic(TOPIC, 1, 1);

        Runnable pTask = () -> {
            try {
                Producer<String, String> producer = new Producer<>(ServiceHelper.loadProperties("producer.props"));
                for (int i = 0; i < COUNT; i++) {
                    ProducerRecord<String, String> pr = new ProducerRecord<>(TOPIC, "Msg", "send " + String.valueOf(i));
                    producer.send(pr);
                    producer.flush();
                }

                producer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        final int[] count = {0};
        Runnable cTask = () -> {
            try {
                Consumer<String, String> consumer = new Consumer<>(ServiceHelper.loadProperties("consumer.props"));
                consumer.subscribe(Collections.singletonList(TOPIC));

                ConsumerRecords<String, String> crs = consumer.poll(2000);

                count[0] = crs.count();
                consumer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        es.execute(cTask);
        es.execute(pTask);

        es.shutdown();

        System.out.println("Waiting es");
        es.awaitTermination(60, TimeUnit.MINUTES);

        try {
            broker.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(COUNT, count[0]);
    }
}
