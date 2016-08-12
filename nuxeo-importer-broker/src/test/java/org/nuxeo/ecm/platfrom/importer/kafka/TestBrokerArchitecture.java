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
import org.nuxeo.ecm.core.api.*;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.importer.kafka.broker.EventBroker;
import org.nuxeo.ecm.platform.importer.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.kafka.importer.ImportOperation;
import org.nuxeo.ecm.platform.importer.kafka.message.Data;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.kafka.producer.Producer;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;
import org.nuxeo.ecm.platform.importer.kafka.settings.Settings;
import org.nuxeo.ecm.platform.importer.source.RandomTextSourceNode;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
@Deploy({ "org.nuxeo.ecm.platform.filemanager.api", //
        "org.nuxeo.ecm.platform.filemanager.core", //
})
public class TestBrokerArchitecture {
    private static final int AMOUNT = 4;
    private static final Log sLog = LogFactory.getLog(TestBrokerArchitecture.class);

    private static EventBroker sBroker;
    private static ExecutorService sProducerService = Executors.newFixedThreadPool(2);
    private static ExecutorService sConsumerService = Executors.newFixedThreadPool(2);

    private static final String TOPIC_MSG = "messenger";
    private static final String TOPIC_ERR = "error";

    private List<Blob> mBlobs;
    private List<Message> mMessages = new LinkedList<>();

    @Inject
    private CoreSession session;

    private ImportOperation operation;

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
        sBroker.stop();
    }

    private Set<String> hashes = Collections.synchronizedSet(new HashSet<>());
    @Before
    public void prepare() throws IOException {
        FileFactory factory = new FileFactory(session);
        mBlobs = factory.preImportBlobs(AMOUNT);

        mMessages = FileFactory.generateFileTree(AMOUNT);

        operation = new ImportOperation(session.getRootDocument(), hashes);

        mMessages.stream().filter(message -> !message.isFolderish()).forEach(message -> {
            int rand = new Random().nextInt(mBlobs.size());
            Blob blob = mBlobs.get(rand);
            try {
                Data data = FileFactory.generateData(blob.getDigest(), blob.getLength());
                message.setData(Collections.singletonList(data));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
//        IntStream.range(0, AMOUNT).forEach(i -> {
//            Message msg = FileFactory.generateMessage(i);
//
//
//            if (!msg.isFolderish()) {
//                int rand = new Random().nextInt(mBlobs.size());
//                Blob blob = mBlobs.get(rand);
//                try {
//                    Data data = FileFactory.generateData(blob.getDigest(), blob.getLength());
//                    msg.setData(Collections.singletonList(data));
//
//
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            mMessages.add(msg);
//        });
    }



    @Test
    public void testShouldReturnDigest() throws IOException {
        String filename = "testDoc";
        new FileFactory(session).createFileDocument(filename);
        DocumentModel model = session.getDocument(new PathRef("/" + filename));
        Blob b = (Blob) model.getProperty("file", "content");
        Assert.assertNotNull(b.getDigest());
    }

    private ForkJoinPool pool = new ForkJoinPool(1);

    @Test
    public void testShouldSendMsgViaBroker() throws IOException, InterruptedException {
        populateProducers();
        populateConsumers();

        sProducerService.awaitTermination(60, TimeUnit.SECONDS);
        sConsumerService.awaitTermination(60, TimeUnit.SECONDS);

        System.out.println("Messages: " + mMessages.size());
        System.out.println("Operations :" + operation.count());
        pool.invoke(operation);

        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.MINUTES);

        DocumentModelList list = session.query("SELECT * FROM Document");
        System.out.println("expected");
        mMessages.forEach(message -> System.out.println(message.getPath() +": "+ message.getTitle()));
        System.out.println("actual");
        traverse(list).forEach(model -> System.out.println(model.getPathAsString() +": " + model.getTitle()));
        Assert.assertEquals(mMessages.size(), traverse(list).size());
    }

    private List<DocumentModel> traverse(DocumentModelList modelList) {
        List<DocumentModel> list = new LinkedList<>();
        if (modelList.size() == 0) return list;
        list.addAll(modelList);
        modelList.forEach(model -> {
            DocumentModelList children = session.getChildren(model.getRef());
            list.addAll(traverse(children));
        });

        return list;
    }

    private void populateProducers() throws IOException {
        Runnable[] tasks = {
            createProducer(TOPIC_MSG, "Msg"),
            createProducer(TOPIC_ERR, "Err")
        };

        for (Runnable r : tasks) {
            sProducerService.execute(r);
        }
        sProducerService.shutdown();
    }


    private Runnable createProducer(String topic, String key) throws IOException {
        return () -> {
            try (Producer<String, Message> p = new Producer<>(ServiceHelper.loadProperties("producer.props"))){
                for (Message msg : mMessages) {
                    ProducerRecord<String, Message> record = new ProducerRecord<>(
                            topic,
                            key,
                            msg
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
                sLog.info(cr.key() + ": " + cr.value());
            }
            return null;
        };

        Runnable[] tasks = {
            createConsumer(TOPIC_MSG, records -> {

                records.forEach(x -> {
                    operation.pushMessage(x.value());

                } );

                return null;
            }),
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

                ConsumerRecords<String, Message> records;

                do {
                    records = c.poll(1000);
                    func.apply(records);
                } while (records.iterator().hasNext());

            } catch (IOException e) {
                sLog.error(e);
            }
        };
    }
}
