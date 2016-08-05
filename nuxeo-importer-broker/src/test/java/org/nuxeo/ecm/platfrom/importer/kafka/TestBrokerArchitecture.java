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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.*;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.importer.kafka.broker.EventBroker;
import org.nuxeo.ecm.platform.importer.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.kafka.producer.Producer;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;
import org.nuxeo.ecm.platform.importer.kafka.settings.Settings;
import org.nuxeo.ecm.platform.importer.source.RandomTextSourceNode;
import org.nuxeo.ecm.platform.importer.source.SourceNode;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
public class TestBrokerArchitecture {
    private static final Log sLog = LogFactory.getLog(TestBrokerArchitecture.class);

    private static final String TOPIC_MSG = "messenger";
    private static final String TOPIC_ERR = "error";

    private ExecutorService mProducerService = Executors.newFixedThreadPool(2);
    private ExecutorService mConsumerService = Executors.newFixedThreadPool(2);

    private SourceNode mMsgNode = RandomTextSourceNode.init(1024, 16, true);
    private SourceNode mErrNode = RandomTextSourceNode.init(1024, 16, true);

    private EventBroker mBroker;

    @BeforeClass
    public static void setUpClass() {
        RandomTextSourceNode.CACHE_CHILDREN = true;
    }

    @Before
    public void setUp() throws Exception {

        System.out.println( String.format(
                "msg: %d\nerr: %d",
                mMsgNode.getChildren().size(),
                mErrNode.getChildren().size()
        ) );


        Map<String, String> props = new HashMap<>();
        props.put(Settings.KAFKA, "kafka.props");
        props.put(Settings.ZOOKEEPER, "zk.props");

        mBroker = new EventBroker(props);
        mBroker.start();

        mBroker.createTopic(TOPIC_MSG, 4, 1);
        mBroker.createTopic(TOPIC_ERR, 4, 1);
    }

    @After
    public void shutdown() throws Exception {
        mProducerService.awaitTermination(120, TimeUnit.SECONDS);
        mConsumerService.awaitTermination(120, TimeUnit.SECONDS);

        mBroker.stop();
    }


    @Test
    public void testShouldWork() throws IOException {
        populateConsumers();
        populateProducers();

        Assert.assertTrue(true);
    }


    private void populateProducers() throws IOException {
        Runnable[] tasks = {
            createProducer(mMsgNode, TOPIC_MSG, "Msg"),
            createProducer(mErrNode, TOPIC_ERR, "Err")
        };

        for (Runnable r : tasks) {
            mProducerService.execute(r);
        }
        mProducerService.shutdown();
    }


    private Runnable createProducer(SourceNode root, String topic, String key) throws IOException {
        List<SourceNode> list = Helper.traverse(root);
        System.out.println("List: " + list.size());

        return () -> {
            try (Producer<String, Message> p = new Producer<>(ServiceHelper.loadProperties("producer.props"))){
                for (SourceNode node : list) {
                    ProducerRecord<String, Message> record = new ProducerRecord<>(
                            topic,
                            key,
                            new Message(node)
                    );

                    p.send(record);
                    p.flush();
                }
            } catch (IOException e) {
                sLog.error(e);
            }
        };
    }


    private void populateConsumers() {
        Function<ConsumerRecords<String, Message>, Void> func = records -> {
            for (ConsumerRecord<String, Message> cr : records) {
                System.out.println(cr.key() + ": " + cr.value());
            }
            return null;
        };

        Runnable[] tasks = {
            createConsumer(TOPIC_MSG, func),
            createConsumer(TOPIC_ERR, func),
        };

        for (Runnable r : tasks) {
            mConsumerService.execute(r);
        }
        mConsumerService.shutdown();
    }


    private Runnable createConsumer(String topic, Function<ConsumerRecords<String, Message>, Void> func) {
        return () -> {
            try (Consumer<String, Message> c = new Consumer<>(ServiceHelper.loadProperties("consumer.props"))) {
                c.subscribe(Collections.singletonList(topic));

                ConsumerRecords<String, Message> records = c.poll(1000);
                func.apply(records);

                while (records.iterator().hasNext()) {
                    func.apply(records);
                    records = c.poll(500);
                }

            } catch (IOException e) {
                sLog.error(e);
            }
        };
    }
}
