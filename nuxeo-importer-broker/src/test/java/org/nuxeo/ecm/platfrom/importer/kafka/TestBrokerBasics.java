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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.nuxeo.ecm.platform.importer.kafka.avro.Message;
import org.nuxeo.ecm.platform.importer.kafka.avro.MessageDeserializer;
import org.nuxeo.ecm.platform.importer.kafka.avro.MessageSerializer;
import org.nuxeo.ecm.platform.importer.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.kafka.producer.Producer;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;
import org.nuxeo.ecm.platfrom.importer.kafka.features.KafkaOneTopicFeature;
import org.nuxeo.runtime.test.runner.Features;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.nuxeo.ecm.platfrom.importer.kafka.features.KafkaOneTopicFeature.TOPIC;


@Features({KafkaOneTopicFeature.class})
public class TestBrokerBasics {

    private static final Log sLog = LogFactory.getLog(TestBrokerBasics.class);

    private static final int COUNT = 100;
    private static final int PARTITION = 2;

    private ExecutorService mExecutorService;

    @Before
    public void setup() {
        mExecutorService = Executors.newFixedThreadPool(4);
    }

    @Test
    public void testShouldUsePassAvroMessages() throws InterruptedException, IOException {
        Properties pp = ServiceHelper.loadProperties("producer.props");
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());

        Runnable pTask = () -> {
            try (Producer<String, Message> producer = new Producer<>(pp)){
                Map<CharSequence, CharSequence> props = new HashMap<>();
                for (int i = 0; i < COUNT / PARTITION; i++) {
                    Message msg = Message.newBuilder()
                            .setType("Folder")
                            .setTitle("Test_" + i)
                            .setFolderish(true)
                            .setHash("hash0000" + i)
                            .setParent("/path/" + i)
                            .setPath("/path/" + i)
                            .setProperties(props)
                            .build();

                    ProducerRecord<String, Message> pr = new ProducerRecord<>(TOPIC, "Msg", msg);
                    producer.send(pr);
                }
                producer.flush();
            }
        };

        for (int i = 0; i < PARTITION; i++) {
            mExecutorService.execute(pTask);
        }

        AtomicInteger count = new AtomicInteger(0);

        Properties cp = ServiceHelper.loadProperties("consumer.props");
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
        Runnable cTask = () -> {
            try (Consumer<String, Message> consumer = new Consumer<>(cp)){
                consumer.subscribe(Collections.singletonList(TOPIC));

                ConsumerRecords<String, Message> records;
                do {
                    records = consumer.poll(2000);
                    count.getAndAccumulate(records.count(), (l,r) -> l+r);
                }while (records.iterator().hasNext());
            }
        };

        for (int i = 0; i < PARTITION; i++) {
            mExecutorService.execute(cTask);
        }

        mExecutorService.shutdown();
        mExecutorService.awaitTermination(60, TimeUnit.MINUTES);

        sLog.info(String.format("Messages received: %d", count.get()));
        assertEquals(COUNT, count.get());
    }
}
