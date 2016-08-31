package org.nuxeo.ecm.platform.importer.kafka.operation;/*
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.platform.importer.kafka.comparator.RecordComparator;
import org.nuxeo.ecm.platform.importer.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.kafka.importer.Importer;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.runtime.transaction.TransactionHelper;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ImportOperation implements Operation  {

    private static final Log log = LogFactory.getLog(ImportOperation.class);
    private final ExecutorService mInternalService = Executors.newCachedThreadPool();

    private String mRepositoryName;
    private Collection<String> mTopics;
    private Properties mConsumerProps;

    public ImportOperation(String repositoryName, Collection<String> topics, Properties consumerProps) {
        mRepositoryName = repositoryName;
        mTopics = topics;
        mConsumerProps = consumerProps;
    }


    @Override
    public Integer call() throws Exception {
        Consumer<String, Message> consumer = new Consumer<>(mConsumerProps);
        consumer.subscribe(mTopics);

        ConsumerRecords<String, Message> records;

        Integer count = 0;

        TransactionHelper.startTransaction();
        CoreSession session = CoreInstance.openCoreSessionSystem(mRepositoryName);
        do {
            List<ConsumerRecord<String, Message>> recordsToRecover = new ArrayList<>();

            records = consumer.poll(1000);

            for (ConsumerRecord<String, Message> record : records) {
                if (!process(session, record.value())) {
                    recordsToRecover.add(record);
                } else {
                    count++;
                }
            }

            Collections.sort(recordsToRecover, new RecordComparator());

            RecoveryOperation operation = new RecoveryOperation(recordsToRecover);
            mInternalService.submit(operation);
        } while (records.iterator().hasNext());
        TransactionHelper.commitOrRollbackTransaction();
        session.close();

        mInternalService.shutdown();
        mInternalService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

        return count;
    }

    @Override
    public boolean process(Message message) {
        TransactionHelper.startTransaction();
        try (CoreSession session = CoreInstance.openCoreSessionSystem(mRepositoryName)){
            new Importer(session).importMessage(message);
            return true;
        } catch (NuxeoException e) {
            log.error(e);
            return false;
        } finally {
            TransactionHelper.commitOrRollbackTransaction();
        }
    }

    private boolean process(CoreSession session, Message message) {
        try {
            new Importer(session).importMessage(message);
            return true;
        } catch (NuxeoException e) {
            log.error(e);
            return false;
        }
    }
}
