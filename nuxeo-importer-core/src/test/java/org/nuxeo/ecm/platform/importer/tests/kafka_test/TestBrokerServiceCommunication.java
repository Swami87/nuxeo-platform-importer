package org.nuxeo.ecm.platform.importer.tests.kafka_test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.nuxeo.ecm.platform.importer.service.kafka.broker.EventBroker;
import org.nuxeo.ecm.platform.importer.service.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.service.kafka.producer.Producer;
import org.nuxeo.ecm.platform.importer.settings.ServiceHelper;
import org.nuxeo.ecm.platform.importer.settings.Settings;

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


public class TestBrokerServiceCommunication {

    private static final int COUNT = 100;
    public static final String TOPIC = "test";
    private static ExecutorService es = Executors.newFixedThreadPool(2);

    @Test
    public void testBrokerComponent() throws Exception {
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

        Runnable cTask = () -> {
            try {
                Consumer<String, String> consumer = new Consumer<>(ServiceHelper.loadProperties("consumer.props"));
                consumer.subscribe(Collections.singletonList(TOPIC));

                ConsumerRecords<String, String> crs = consumer.poll(2000);
                assertEquals(COUNT, crs.count());
                consumer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        es.execute(cTask);
//        Thread.sleep(1000);
        es.execute(pTask);

        es.shutdown();

        System.out.println("Waiting es");
        es.awaitTermination(60, TimeUnit.MINUTES);

        try {
            broker.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testShouldCreateProducer() throws IOException {

        Properties props = ServiceHelper.loadProperties("producer.props");

        Producer<String, String> producer = new Producer<>(props);
        assertNotNull(producer);
    }
}
