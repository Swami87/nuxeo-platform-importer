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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.importer.kafka.broker.EventBroker;
import org.nuxeo.ecm.platform.importer.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.kafka.importer.Importer;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.kafka.producer.Producer;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;
import org.nuxeo.ecm.platform.importer.kafka.settings.Settings;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.transaction.TransactionHelper;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
public class TestImporter {

    private static final Log log = LogFactory.getLog(TestImporter.class);
    private static final String TOPIC = "test";
    private static final int NODES = 100;

    private ExecutorService mService = Executors.newSingleThreadExecutor();

    private Producer<String, Message> mProducer;
    private Consumer<String, Message> mConsumer;

    private List<Message> mMessages;


    @Inject
    CoreSession mCoreSession;

    private EventBroker mBroker;

    @Before
    public void setUp() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(Settings.KAFKA, "kafka.props");
        props.put(Settings.ZOOKEEPER, "zk.props");

        mBroker = new EventBroker(props);
        mBroker.start();
        mBroker.createTopic(TOPIC, 4, 1);

        mProducer = new Producer<>(ServiceHelper.loadProperties("producer.props"));
        mConsumer = new Consumer<>(ServiceHelper.loadProperties("consumer.props"));

        mMessages = new FileFactory(mCoreSession).createDocumentsOnServer(NODES);

        mConsumer.subscribe(Collections.singletonList(TOPIC));
        runProducerService();
    }

    @After
    public void shutdown() throws Exception {
        if (mService != null) {
            mService.awaitTermination(1, TimeUnit.SECONDS);
        }

        mProducer.close();
        mConsumer.close();
        mBroker.stop();
    }


    @Test
    public void testShouldProduce() throws IOException, InterruptedException {
        int records = runConsumerService();
        Assert.assertTrue(records > 0);
    }


    @Test
    public void testShouldImport() throws IOException, InterruptedException {
        AtomicInteger count = new AtomicInteger(0);

        ConsumerRecords<String, Message> records;
        do {
            records = mConsumer.poll(1000);
            records.forEach(rec -> {
                count.incrementAndGet();
                new Importer(mCoreSession, rec.value()).runImport();
            });
        } while (records.iterator().hasNext());

        TransactionHelper.startTransaction();
        DocumentModelList list = mCoreSession.query("select * from File");
        TransactionHelper.commitOrRollbackTransaction();
        Assert.assertEquals(mMessages.size(), list.size());
    }

    private void runProducerService() {
        mService = Executors.newSingleThreadExecutor();

        Runnable task = () -> {
            for (Message message : mMessages) {
                ProducerRecord<String, Message> record = new ProducerRecord<>(TOPIC, 0, 100L, "Node", message);

                mProducer.send(record);
                mProducer.flush();
            }
        };
        mService.execute(task);
        mService.shutdown();
    }


    private int runConsumerService() throws InterruptedException, IOException {
        runProducerService();
        AtomicInteger count = new AtomicInteger(0);

        ConsumerRecords<String, Message> records;
        do {
            records = mConsumer.poll(1000);
            count.addAndGet(records.count());
        } while (records.iterator().hasNext());

        mService.awaitTermination(60, TimeUnit.SECONDS);

        return count.intValue();
    }
}
