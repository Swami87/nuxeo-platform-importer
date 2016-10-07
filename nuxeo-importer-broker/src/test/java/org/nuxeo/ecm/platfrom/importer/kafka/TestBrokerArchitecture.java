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
import java.util.concurrent.TimeUnit;


@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
@Deploy({ "org.nuxeo.ecm.platform.filemanager.api", //
        "org.nuxeo.ecm.platform.filemanager.core", //
})
public class TestBrokerArchitecture {
    private static final Log sLog = LogFactory.getLog(TestBrokerArchitecture.class);

    private static final int MAX_AMOUNT_OF_CHILDREN = 25;
    private static final int THREADS = 4;
    private static final List<String> topicLevels = Arrays.asList("One", "Two", "Three", "Four");
    private static Integer toImport = 1;

    private static EventBroker sBroker;

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

        for (String level : topicLevels) {
            sBroker.createTopic(level, THREADS, 1);
        }

        Message root = FileFactory.generateRoot();

        Producer<String, Message> producer = new Producer<>(ServiceHelper.loadProperties("producer.props"));
        producer.send(new ProducerRecord<>(topicLevels.get(0), "msg", root));
        producer.flush();

        List<Message> lastImport = new ArrayList<>();
        lastImport.add(root);

        for (String topic : topicLevels.subList(1, topicLevels.size())) {
            int rand = new Random().nextInt(MAX_AMOUNT_OF_CHILDREN) + 1;
            List<Message> generatedLevel = FileFactory.generateLevel(lastImport, rand);
            for (Message msg : generatedLevel) {
                producer.send(new ProducerRecord<>(topic, "msg", msg), (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });
                producer.flush();
            }
            toImport += generatedLevel.size();
            lastImport.clear();
            lastImport.addAll(generatedLevel);
        }
        producer.close();
    }


    @AfterClass
    public static void shutdown() throws Exception {
        sBroker.stop();
    }

    @Before
    public void prepare() throws IOException {
        FileFactory factory = new FileFactory();
        mBlobsData = factory.preImportBlobs(MAX_AMOUNT_OF_CHILDREN);
    }


    @Test
    public void testShouldImportLevelByLevel() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Properties props = ServiceHelper.loadProperties("consumer.props");
        ImportManager manager = new ImportManager.Builder(session.getRepositoryName())
                .threads(THREADS)
                .consumer(props)
                .build();

        ImportManager.Result result = manager.syncImport(topicLevels);
        int imported = result.getImported();

        System.out.println(
                String.format("Import of %d Documents finished in %f;\n" +
                                "Speed %.2f docs/s;\n" +
                                "------------------------------------\n" +
                                "Recovered %d",
                        imported,
                        stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1e3,
                        (1.0 * imported) / stopwatch.elapsed(TimeUnit.SECONDS),
                        result.getRecovered()
                )
        );

        Assert.assertEquals(toImport.intValue(), imported);
    }
}
