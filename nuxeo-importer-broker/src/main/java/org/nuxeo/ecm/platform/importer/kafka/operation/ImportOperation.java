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
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.api.UnrestrictedSessionRunner;
import org.nuxeo.ecm.platform.importer.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.kafka.importer.Importer;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.runtime.transaction.TransactionHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ImportOperation implements Callable<Integer> {

    private static final Log log = LogFactory.getLog(ImportOperation.class);
    private final ExecutorService mInternalService = Executors.newCachedThreadPool();

    private String mRepositoryName;
    private Collection<String> mTopics;
    private Properties mConsumerProps;
    private BlockingQueue<ConsumerRecord<String, Message>> mRecoveryQueue;

    public ImportOperation(String repositoryName, Collection<String> topics, Properties consumerProps, BlockingQueue<ConsumerRecord<String, Message>> queue) {
        mRepositoryName = repositoryName;
        mTopics = topics;
        mConsumerProps = consumerProps;
        mRecoveryQueue = queue;
    }

    @Override
    public Integer call() throws Exception {
        Consumer<String, Message> consumer = new Consumer<>(mConsumerProps);
        consumer.subscribe(mTopics);

        ConsumerRecords<String, Message> records;

        Integer count = 0;

        TransactionHelper.startTransaction();
        do {
            records = consumer.poll(1000);

            List<ConsumerRecord<String, Message>> recoverList = process(mRepositoryName, records);
            count += (records.count() - recoverList.size());
            for (ConsumerRecord<String, Message> record : recoverList) mRecoveryQueue.put(record);
        } while (records.iterator().hasNext());
        TransactionHelper.commitOrRollbackTransaction();

        mRecoveryQueue.put(new ConsumerRecord<>("empty", 0,0,"POISON", null));

        consumer.close();

        mInternalService.shutdown();
        mInternalService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

        return count;
    }


    private List<ConsumerRecord<String, Message>> process(String repositoryName, ConsumerRecords<String, Message> records) throws InterruptedException {
        List<ConsumerRecord<String, Message>> toRecover = new ArrayList<>();
        AtomicBoolean isRunning = new AtomicBoolean(true);
        UnrestrictedSessionRunner runner = new UnrestrictedSessionRunner(repositoryName) {

            @Override
            public void run() {
                for (ConsumerRecord<String, Message> record : records) {
                    try {
                        new Importer(session).importMessage(record.value());
                    } catch (NuxeoException e) {
                        log.error(e);
                        toRecover.add(record);
                    }
                }

                isRunning.set(false);
            }
        };

        runner.runUnrestricted();
        while (isRunning.get()) Thread.sleep(100);
        return toRecover;
    }
}
