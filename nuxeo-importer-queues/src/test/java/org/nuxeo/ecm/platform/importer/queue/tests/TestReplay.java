/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
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
 */
package org.nuxeo.ecm.platform.importer.queue.tests;

import com.google.inject.Inject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.importer.filter.EventServiceConfiguratorFilter;
import org.nuxeo.ecm.platform.importer.filter.ImporterFilter;
import org.nuxeo.ecm.platform.importer.log.BufferredLogger;
import org.nuxeo.ecm.platform.importer.log.ImporterLogger;
import org.nuxeo.ecm.platform.importer.queue.QueueImporter;
import org.nuxeo.ecm.platform.importer.queue.consumer.ConsumerFactory;
import org.nuxeo.ecm.platform.importer.queue.manager.RandomQueuesManager;
import org.nuxeo.ecm.platform.importer.queue.producer.Producer;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.transaction.TransactionHelper;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
public class TestReplay {

    protected static final Log log = LogFactory.getLog(TestImporter.class);

    @Inject
    CoreSession session;

    @Test
    public void shouldImportAllNonBuggyNodes() throws InterruptedException {
        importWithBuggyNodes(0, 0);
    }

    @Test
    public void shouldImportAllNonBuggyNodesWithSlowConsumer() throws InterruptedException {
        importWithBuggyNodes(0, 100);
    }

    @Test
    public void shouldImportAllNonBuggyNodesWithSlowProducer() throws InterruptedException {
        importWithBuggyNodes(100, 0);
    }

    private void importWithBuggyNodes(int producerDelayMs, int consumerDelayMs) {
        ImporterLogger logger = mock(ImporterLogger.class);
        // To get logs
        // ImporterLogger logger = new BufferredLogger(log);
        QueueImporter importer = new QueueImporter(logger);
        ImporterFilter filter = new EventServiceConfiguratorFilter(true, false, true, false, true);
        importer.addFilter(filter);
        RandomQueuesManager qm = new RandomQueuesManager(logger, 7, 10);

        // Given a producer that generate some buggy nodes.
        // index-0, index-30, index-50, index-60 and index-90 should not be created
        Producer producer = new BuggyNodeProducer(logger, 100, 30, 50, producerDelayMs, 0);
        ConsumerFactory fact = new BuggyConsumerFactory(consumerDelayMs);

        // When the importer launches the import
        importer.importDocuments(producer, qm, "/", session.getRepositoryName(), 9, fact);

        // Commit for visibility with repeatable read isolation (mysql)
        TransactionHelper.commitOrRollbackTransaction();
        TransactionHelper.startTransaction();

        // Then only buggy nodes should'nt be imported.
        DocumentModelList docs = session.query("SELECT * FROM File");
        int expected = 95;
        if (expected != docs.size()) {
            for (DocumentModel doc : docs) {
                System.out.println(doc.getName());
            }
        }
        assertEquals("Count of documents that should have been created after import", expected, docs.size());
        // verify(logger, times(20)).error(anyString());
    }



}
