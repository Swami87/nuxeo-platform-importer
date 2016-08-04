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
    private static final int NODES = 1000;

    private ExecutorService mService = Executors.newSingleThreadExecutor();

    RandomTextSourceNode mRoot;
    private List<SourceNode> mTraversedList;

    private Producer<String, Message> mProducer;
    private Consumer<String, Message> mConsumer;

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

        mRoot = RandomTextSourceNode.init(NODES, 512, true);
        mTraversedList = Helper.traverseList(mRoot.getChildren());
        mConsumer.subscribe(Collections.singletonList(TOPIC));
        runProducerService();
    }

    @After
    public void shutdown() throws Exception {
        if (mService != null) {
            mService.awaitTermination(5, TimeUnit.MINUTES);
        }

        mProducer.close();
        mConsumer.close();
        mBroker.stop();
    }


    @Test
    public void testShouldProduce() throws IOException, InterruptedException {
        int records = executeTransaction();
        Assert.assertTrue(records > 0);
    }


    @Test
    public void testShouldNotCreateDuplicates() {
        Set<Message> nodes = new HashSet<>();

        int count = 0;

        while (count < mTraversedList.size()) {

            ConsumerRecords<String, Message> records =  mConsumer.poll(100);

            for (ConsumerRecord<String, Message> record : records) {
                nodes.add(record.value());
            }

            count += records.count();
        }

        Assert.assertEquals(count, nodes.size());
    }


    @Test
    public void testShouldImport() throws IOException, InterruptedException {
        DocumentModel model = mCoreSession.getRootDocument();

        int count = 0;

        while (count < mTraversedList.size()) {

            ConsumerRecords<String, Message> records =  mConsumer.poll(100);

            for (ConsumerRecord<String, Message> record : records) {
//                System.out.println(record.value());
                Importer importer = new Importer(model, record.value());
                importer.runImport();
            }

            count += records.count();
        }

        model = mCoreSession.getRootDocument();
        DocumentModelList list = mCoreSession.getChildren(model.getRef());
        Assert.assertTrue(list.size() >= mRoot.getChildren().size());
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
            try {
                System.out.println("List size = " + mTraversedList.size());
                for (SourceNode child : mTraversedList) {
                    ProducerRecord<String, Message> record = new ProducerRecord<>(TOPIC, 0, 100L, "Node", new Message(child));

                    mProducer.send(record);
                    mProducer.flush();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
        mService.execute(task);
        mService.shutdown();
    }


    private int executeTransaction() throws InterruptedException, IOException {
        runProducerService();

        int count = 0;

        while (count < mTraversedList.size()) {

            ConsumerRecords<String, Message> records =  mConsumer.poll(100);

            count += records.count();
        }

        mService.awaitTermination(60, TimeUnit.SECONDS);

        return count;
    }


}
