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
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.importer.kafka.broker.EventBroker;
import org.nuxeo.ecm.platform.importer.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.kafka.importer.ImportManager;
import org.nuxeo.ecm.platform.importer.kafka.message.Data;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.kafka.producer.Producer;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;
import org.nuxeo.ecm.platform.importer.kafka.settings.Settings;
import org.nuxeo.ecm.platform.importer.source.RandomTextSourceNode;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.transaction.TransactionHelper;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;


@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
@Deploy({ "org.nuxeo.ecm.platform.filemanager.api", //
        "org.nuxeo.ecm.platform.filemanager.core", //
})
public class TestBrokerArchitecture {
    private static final int AMOUNT = 10;
    private static final Log sLog = LogFactory.getLog(TestBrokerArchitecture.class);

    private static EventBroker sBroker;
    private ForkJoinPool mImporterPool = ForkJoinPool.commonPool();
    private static ExecutorService sProducerService = Executors.newFixedThreadPool(2);
    private static ExecutorService sConsumerService = Executors.newFixedThreadPool(2);

    private static final String TOPIC_MSG = "messenger";
    private static final String TOPIC_ERR = "error";

    private List<Data> mBlobsData;
    private List<Message> mMessages = new ArrayList<>();

    @Inject
    private CoreSession session;

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

    @Before
    public void prepare() throws IOException {
        FileFactory factory = new FileFactory();
        mBlobsData = factory.preImportBlobs(AMOUNT);

        mMessages = FileFactory.generateFileTree(AMOUNT);

        mMessages.stream().filter(message -> !message.isFolderish()).forEach(message -> {
            int rand = new Random().nextInt(mBlobsData.size());
            Data data = mBlobsData.get(rand);
            message.setData(Collections.singletonList(data));
        });
    }

    @Test
    public void testShouldImportViaManager() throws Exception {
        populateProducers();

        ImportManager manager = new ImportManager(session, 1);
        manager.start(1, TOPIC_MSG);

        check();
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

    private void check() {
        if (!TransactionHelper.isTransactionActive()) TransactionHelper.startTransaction();
        DocumentModelList list = session.getChildren(session.getRootDocument().getRef());
        List<DocumentModel> traversedList = Helper.traverse(list, session);

        List<String> names = mMessages.stream()
                .map(Helper::getFullPath)
                .sorted()
                .collect(Collectors.toList());

        List<String> models = traversedList.stream()
                .map(DocumentModel::getPathAsString)
                .sorted()
                .collect(Collectors.toList());

        Assert.assertArrayEquals(names.toArray(), models.toArray());
        TransactionHelper.commitOrRollbackTransaction();
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
