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
package org.nuxeo.ecm.platform.importer.kafka.importer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentNotFoundException;
import org.nuxeo.ecm.platform.importer.kafka.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.runtime.transaction.TransactionHelper;

import java.util.concurrent.RecursiveAction;

public class ImportOperation extends RecursiveAction {

    private CoreSession mSession;
    private Consumer<String, Message> mConsumer;
    private ConsumerRecords<String, Message> mRecords;

    public ImportOperation(CoreSession session,
                                   Consumer<String, Message> consumer,
                                   ConsumerRecords<String, Message> records) {
        mSession = session;
        mConsumer = consumer;
        mRecords = records;
    }

    @Override
    protected void compute() {
        mRecords = mConsumer.poll(1000);
        if (mRecords.iterator().hasNext()) {
            new ImportOperation(mSession, mConsumer, mRecords).fork();
        }
        for (ConsumerRecord<String, Message> record : mRecords) {
            importMessage(record.value());
        }
    }

    private void importMessage(Message message) {
        try {

            if (TransactionHelper.isTransactionActive()) {
                new Importer(mSession).importMessage(message);
            } else {
                TransactionHelper.startTransaction();
                new Importer(mSession).importMessage(message);
                TransactionHelper.commitOrRollbackTransaction();
            }
        } catch (DocumentNotFoundException e) {
            e.printStackTrace();
        }
    }

}
