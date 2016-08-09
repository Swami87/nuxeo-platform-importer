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
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.PathRef;
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
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import javax.inject.Inject;
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
@Deploy({ "org.nuxeo.ecm.platform.filemanager.api", //
        "org.nuxeo.ecm.platform.filemanager.core", //
})
public class TestBrokerArchitecture {
    private static final Log sLog = LogFactory.getLog(TestBrokerArchitecture.class);

    private static EventBroker sBroker;
    private static ExecutorService sProducerService = Executors.newFixedThreadPool(2);
    private static ExecutorService sConsumerService = Executors.newFixedThreadPool(2);

    private static final String TOPIC_MSG = "messenger";
    private static final String TOPIC_ERR = "error";

    private SourceNode mMsgNode = RandomTextSourceNode.init(1024, 16, true);
    private SourceNode mErrNode = RandomTextSourceNode.init(1024, 16, true);

    @Inject
    CoreSession session;

    @BeforeClass
    public static void setUpClass() throws Exception {
        RandomTextSourceNode.CACHE_CHILDREN = true;

        Map<String, String> props = new HashMap<>();
        props.put(Settings.KAFKA, "kafka.props");
        props.put(Settings.ZOOKEEPER, "zk.props");

        sBroker = new EventBroker(props);
        sBroker.start();

        sBroker.createTopic(TOPIC_MSG, 4, 1);
        sBroker.createTopic(TOPIC_ERR, 4, 1);
    }


    @AfterClass
    public static void shutdown() throws Exception {
        sProducerService.awaitTermination(5, TimeUnit.SECONDS);
        sConsumerService.awaitTermination(5, TimeUnit.SECONDS);

        sBroker.stop();
    }



    @Test
    public void testShouldReturnDigest() throws IOException {
        String filename = "testDoc";
        new FileFactory(session).createFileDocument(filename);
        DocumentModel model = session.getDocument(new PathRef("/" + filename));
        Blob b = (Blob) model.getProperty("file", "content");
        Assert.assertNotNull(b.getDigest());
    }

    @Test
    public void testShouldCreateLotsOfDigests() {
        FileFactory factory = new FileFactory(session);

        List<String> dgsts = factory.generateFiles(100);

        dgsts.forEach(System.out::println);
    }


    private void populateProducers() throws IOException {
        Runnable[] tasks = {
            createProducer(mMsgNode, TOPIC_MSG, "Msg"),
            createProducer(mErrNode, TOPIC_ERR, "Err")
        };

        for (Runnable r : tasks) {
            sProducerService.execute(r);
        }
        sProducerService.shutdown();
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
            sConsumerService.execute(r);
        }
        sConsumerService.shutdown();
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
