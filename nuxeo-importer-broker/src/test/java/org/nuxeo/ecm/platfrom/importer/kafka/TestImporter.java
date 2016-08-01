package org.nuxeo.ecm.platfrom.importer.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.PathRef;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.importer.kafka.broker.EventBroker;
import org.nuxeo.ecm.platform.importer.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.kafka.producer.Producer;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;
import org.nuxeo.ecm.platform.importer.kafka.settings.Settings;
import org.nuxeo.ecm.platform.importer.source.RandomTextSourceNode;
import org.nuxeo.ecm.platform.importer.source.SourceNode;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * Created by anechaev on 7/29/16.
 * © Andrei Nechaev 2016
 */

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
public class TestImporter {

    private static final Log log = LogFactory.getLog(TestImporter.class);
    private static final String TOPIC = "test";

    private ExecutorService mService = Executors.newSingleThreadExecutor();

    private List<SourceNode> mChildren;

    private Producer<String, SourceNode> mProducer;
    private Consumer<String, SourceNode> mConsumer;

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
        mBroker.createTopic(TOPIC, 1, 1);

        mProducer = new Producer<>(ServiceHelper.loadProperties("producer.props"));
        mConsumer = new Consumer<>(ServiceHelper.loadProperties("consumer.props"));

        RandomTextSourceNode mRoot = RandomTextSourceNode.init(100, 1, true);
        mChildren = mRoot.getChildren();
    }

    @After
    public void shutdown() throws Exception {
        mProducer.close();
        mConsumer.close();
        mBroker.stop();
    }


    @Test
    public void testShouldProduce() throws IOException, InterruptedException {
        mConsumer.subscribe(Collections.singletonList(TOPIC));

        int records = executeTransaction();
        Assert.assertTrue(records > 0);
    }


    @Test
    public void testConsumerShouldReceiveAllMsg() throws InterruptedException {
        mConsumer.subscribe(Collections.singletonList(TOPIC));

        int records = executeTransaction();

        Assert.assertEquals(mChildren.size(), records);
    }


    @Test
    public void testShouldNotCreateDuplicates() {
        Set<SourceNode> nodes = new HashSet<>();
        mConsumer.subscribe(Collections.singletonList(TOPIC));

        runProducerService();

        int count = 0;

        while (count < mChildren.size()) {

            ConsumerRecords<String, SourceNode> records =  mConsumer.poll(100);

            for (ConsumerRecord<String, SourceNode> record : records) {
                System.out.println(record.value().getName());
                nodes.add(record.value());
            }

            count += records.count();
        }

        Assert.assertEquals(count, nodes.size());
    }


    @Test
    public void testShouldImport() throws IOException {
        DocumentModel model = mCoreSession.getRootDocument();
        DocumentModel root = mCoreSession.getDocument(new PathRef("/"));


        for (SourceNode ch : mChildren) {
            printNodes(ch);
        }
//        runProducerService();
//
//        int count = 0;
//
//        while (count < mChildren.size()) {
//
//            ConsumerRecords<String, SourceNode> records =  mConsumer.poll(100);
//
//            for (ConsumerRecord<String, SourceNode> record : records) {
//                printNodes(record.value());
//            }
//
//            count += records.count();
//        }
    }

    private void printNodes(SourceNode node) throws IOException {
        if (node == null) return;

        System.out.println(node.getName());
        if (node.getChildren() != null) {
            for (SourceNode n : node.getChildren()) {
                printNodes(n);
            }
        }
    }


    private void runProducerService() {
        mService = Executors.newSingleThreadExecutor();

        Runnable task = () -> {
            for (SourceNode child : mChildren) {
                ProducerRecord<String, SourceNode> record = new ProducerRecord<>(TOPIC, 0, 100L, "Node", child);
                mProducer.send(record);
                mProducer.flush();
            }
        };
        mService.execute(task);
        mService.shutdown();
    }

    private int executeTransaction() throws InterruptedException {
        runProducerService();

        int count = 0;

        while (count < mChildren.size()) {

            ConsumerRecords<String, SourceNode> records =  mConsumer.poll(100);

            count += records.count();
        }


        mService.awaitTermination(60, TimeUnit.SECONDS);

        return count;
    }


}
