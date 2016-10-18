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

package org.nuxeo.ecm.platfrom.importer.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.nuxeo.ecm.platform.importer.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.kafka.producer.Producer;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;
import org.nuxeo.runtime.test.runner.Features;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;


@Features({KafkaFeature.class})
public class TestBrokerBasics {

    private static final int COUNT = 100;
    private static final String TOPIC = "test";
    private static final int PARTITION = 4;

    private static ExecutorService es = Executors.newFixedThreadPool(5);

    @Test
    public void testShouldSendAndReceiveMsgViaBroker() throws Exception {
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

        for (int i = 0; i < PARTITION; i++) {
            es.execute(pTask);
        }

        AtomicInteger count = new AtomicInteger(0);
        Runnable cTask = () -> {
            try {
                Properties cp = ServiceHelper.loadProperties("consumer.props");
                cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

                Consumer<String, String> consumer = new Consumer<>(cp);
                consumer.subscribe(Collections.singletonList(TOPIC));

                while (true) {
                    ConsumerRecords<String, String> crs = consumer.poll(5000);

                    count.getAndAccumulate(crs.count(), (l,r) -> l+r);

                    for (ConsumerRecord<String, String> record : crs) {
                        System.out.println(record.value());
                    }
                    if (!crs.iterator().hasNext()) break;
                }

                consumer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        es.execute(cTask);

        es.shutdown();
        es.awaitTermination(60, TimeUnit.MINUTES);

        System.out.println(String.format("Messages received: %d", count.get()));
        assertEquals(COUNT, count.get());
    }
}
