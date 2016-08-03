package org.nuxeo.ecm.platfrom.importer.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by anechaev on 7/28/16.
 * © Andrei Nechaev 2016
 */


public class TestBrokerBasics {

    private static final int COUNT = 100;
    private static final String TOPIC = "test";
    private static final int PARTITION = 5;

    private static ExecutorService es = Executors.newFixedThreadPool(6);

    private EventBroker mBroker;

    @Before
    public void setUp() throws Exception {
        Map<String, String> brokerProps = new HashMap<>();
        brokerProps.put(Settings.KAFKA, "kafka.props");
        brokerProps.put(Settings.ZOOKEEPER, "zk.props");

        mBroker = new EventBroker(brokerProps);

        mBroker.start();
    }

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
    public void testShouldSendAndReceiveMsgViaBroker() throws Exception {

        mBroker.createTopic(TOPIC, PARTITION, 1);

        Runnable pTask = () -> {
            try {
                Properties pp = ServiceHelper.loadProperties("producer.props");
                pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                Producer<String, String> producer = new Producer<>(pp);
                for (int i = 0; i < COUNT / PARTITION; i++) {
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
                Properties cp = ServiceHelper.loadProperties("consumer.props");
                cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

                Consumer<String, String> consumer = new Consumer<>(cp);
                consumer.subscribe(Collections.singletonList(TOPIC));

                ConsumerRecords<String, String> crs = consumer.poll(2000);

                count[0] = crs.count();
                consumer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        es.execute(cTask);

        for (int i = 0; i < PARTITION; i++) {
            es.execute(pTask);
        }

        es.shutdown();

        System.out.println("Waiting es");
        es.awaitTermination(60, TimeUnit.MINUTES);

        try {
            mBroker.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Messages received: %d", count[0]));
        assertEquals(COUNT, count[0]);
    }
}
