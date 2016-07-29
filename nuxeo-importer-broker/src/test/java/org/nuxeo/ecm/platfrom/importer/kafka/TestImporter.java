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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private RandomTextSourceNode root = RandomTextSourceNode.init(1000, 1, true);
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

        mChildren = root.getChildren();
    }

    @After
    public void shutdown() throws Exception {
        mProducer.close();
        mConsumer.close();
        mBroker.stop();
    }

    @Test
    public void testConsumerShouldReceiveAllMsg() throws InterruptedException {
        ConsumerRecords<String, SourceNode> records = executeTransaction();
        Assert.assertEquals(mChildren.size(), records.count());
    }

    @Test
    public void testShouldProduce() throws IOException, InterruptedException {
        ConsumerRecords<String, SourceNode> records = executeTransaction();
        Assert.assertTrue(records.count() > 0);
    }

    private ConsumerRecords<String, SourceNode> executeTransaction() throws InterruptedException {
        ExecutorService service = Executors.newSingleThreadExecutor();

        mConsumer.subscribe(Collections.singletonList(TOPIC));

        Runnable task = () -> {
            for (SourceNode child : mChildren) {
                ProducerRecord<String, SourceNode> record = new ProducerRecord<>(TOPIC, 0, 100L, "Node", child);
                mProducer.send(record);
                mProducer.flush();
            }
        };
        service.execute(task);
        service.shutdown();
        ConsumerRecords<String, SourceNode> records =  mConsumer.poll(4000);

        service.awaitTermination(60, TimeUnit.SECONDS);

        return records;
    }


}
