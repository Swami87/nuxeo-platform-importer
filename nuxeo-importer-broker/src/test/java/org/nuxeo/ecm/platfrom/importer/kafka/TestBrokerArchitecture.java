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

import com.google.common.base.Stopwatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.*;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.importer.kafka.broker.EventBroker;
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

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
@Deploy({ "org.nuxeo.ecm.platform.filemanager.api", //
        "org.nuxeo.ecm.platform.filemanager.core", //
})
public class TestBrokerArchitecture {
    private static final Log sLog = LogFactory.getLog(TestBrokerArchitecture.class);

    private static final int AMOUNT = 1000;
    private static final int THREADS = 2;
    private static final String TOPIC_NAME = "messenger";

    private static EventBroker sBroker;
    private static ExecutorService sProducerService = Executors.newFixedThreadPool(1);

    private List<Data> mBlobsData;

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

        sBroker.createTopic(TOPIC_NAME, THREADS, 1);
    }


    @AfterClass
    public static void shutdown() throws Exception {
        sBroker.stop();
    }

    @Before
    public void prepare() throws IOException {
        FileFactory factory = new FileFactory();
        mBlobsData = factory.preImportBlobs(AMOUNT);
    }

    @Test
    public void testShouldImportViaManager() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        populateProducers();

        Properties props = ServiceHelper.loadProperties("consumer.props");
        ImportManager manager = new ImportManager.Builder(session.getRepositoryName())
                .threads(THREADS)
                .consumer(props)
                .build();

        manager.start(THREADS, TOPIC_NAME);
        ImportManager.Result result = manager.waitUntilStop();

        stopwatch.stop();

        Integer docs = FileFactory.counter.get();
        System.out.println(
                String.format("Import of %d Documents finished in %f;" +
                                "\nSpeed %.2f docs/s;\n" +
                                "------------------------------------\n" +
                                "\nRecovered %d",
                        docs,
                        stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1e3,
                        (1.0 * docs) / stopwatch.elapsed(TimeUnit.SECONDS),
                        result.getRecovered()
                        )
        );

        Assert.assertEquals(docs.longValue(), result.getImported().longValue());
    }

    @Test
    public void testShouldImportLevelByLevel() throws Exception {
        List<String> topicLevels = Arrays.asList("One", "Two", "Three", "Four");

        for (String level : topicLevels) {
            sBroker.createTopic(level, THREADS, 1);
        }

        Message root = FileFactory.generateRoot();

        Producer<String, Message> producer = new Producer<>(ServiceHelper.loadProperties("producer.props"));
        producer.send(new ProducerRecord<>(topicLevels.get(0), "msg", root));

        int toImport = 1;
        List<Message> lastImport = Collections.singletonList(root);
        for (int i = 1; i < topicLevels.size(); i++) {
            int rand = new Random().nextInt(AMOUNT);
            List<Message> generatedLevel = FileFactory.generateLevel(lastImport, rand);
            for (Message msg : generatedLevel) {
                producer.send(new ProducerRecord<>(topicLevels.get(i), "msg", msg));
            }
            toImport += generatedLevel.size();
            lastImport = generatedLevel;
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        int imported = 0;
        for (String topic : topicLevels) {
            Properties props = ServiceHelper.loadProperties("consumer.props");
            ImportManager manager = new ImportManager.Builder(session.getRepositoryName())
                    .threads(THREADS)
                    .consumer(props)
                    .build();

            manager.start(THREADS, topic);
            ImportManager.Result result = manager.waitUntilStop();
            imported += result.getImported();

            System.out.println("Recovered: " + result.getRecovered());
        }
        System.out.println(
                String.format("Import of %d Documents finished in %f;\n" +
                                "Speed %.2f docs/s;\n" +
                                "------------------------------------\n" +
                                "Recovered %d",
                        imported,
                        stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1e3,
                        (1.0 * imported) / stopwatch.elapsed(TimeUnit.SECONDS),
                        imported
                )
        );


        Assert.assertEquals(toImport, imported);
    }


    private void populateProducers() throws IOException {
        sProducerService.execute(createProducer(TOPIC_NAME));
        sProducerService.shutdown();
    }

    private Runnable createProducer(String topic) throws IOException {
        return () -> FileFactory.generateTree(mBlobsData, topic, AMOUNT);
    }
}
