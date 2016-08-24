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
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentNotFoundException;
import org.nuxeo.ecm.platform.importer.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.kafka.importer.Importer;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ImportOperation implements Callable<Integer> {

    private static final Log log = LogFactory.getLog(ImportOperation.class);
    private final ExecutorService mInternalService = Executors.newCachedThreadPool();

    private CoreSession mCoreSession;
    private Collection<String> mTopics;
    private Consumer<String, Message> mConsumer;

    public ImportOperation(CoreSession coreSession, Collection<String> topics) {
        mCoreSession = coreSession;
        mTopics = topics;
    }


    private boolean process(Message message) {
        try {
            new Importer(mCoreSession).importMessage(message);
            return true;
        } catch (DocumentNotFoundException e) {
            log.error(e);
            return false;
        }
    }

    @Override
    public Integer call() throws Exception {
        try {
            mConsumer = new Consumer<>(ServiceHelper.loadProperties("consumer.props"));
        } catch (IOException e) {
            log.error(e);
        }
        mConsumer.subscribe(mTopics);

        ConsumerRecords<String, Message> records;

        Integer count = 0;
        do {
            records = mConsumer.poll(1000);

            Set<ConsumerRecord<String, Message>> recoverySet = new LinkedHashSet<>();
            for (ConsumerRecord<String, Message> record : records) {
                if (!process(record.value())) {
                    recoverySet.add(record);
                } else {
                    count++;
                }
            }

            RecoveryOperation operation = new RecoveryOperation(recoverySet);
            mInternalService.execute(operation);
        } while (records.iterator().hasNext());

        mInternalService.shutdown();
        try {
            mInternalService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error(e);
        }

        return count;
    }
}
