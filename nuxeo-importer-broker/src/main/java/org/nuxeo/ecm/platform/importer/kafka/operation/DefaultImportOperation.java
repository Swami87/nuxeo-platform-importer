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
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentNotFoundException;
import org.nuxeo.ecm.platform.importer.kafka.importer.Importer;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.runtime.transaction.TransactionHelper;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.RecursiveAction;


public class DefaultImportOperation extends RecursiveAction {
    private static final Log sLogger = LogFactory.getLog(DefaultImportOperation.class);

    private Collection<Message> mMessages;
    private CoreSession mSession;

    private String mHash;

    private Comparator<Message> mComparator = (msg1, msg2) -> msg1.getPath().compareTo(msg2.getPath());

    public DefaultImportOperation(CoreSession session, Collection<Message> messages) {
        this.mSession = session;
        mMessages = messages;
    }

    @Override
    protected void compute() {
        if (mMessages.size() < 1) return;

        mMessages.stream()
                .sorted(mComparator)
                .filter(message -> message.getParentHash() == null || message.getParentHash().equals(mHash))
                .forEach(message -> {
                    importMessage(message);
                    mMessages.remove(message);
                    DefaultImportOperation operation = new DefaultImportOperation(mSession, mMessages);
                    operation.setHash(message.getHash());
                    operation.fork();
                });
    }

    private void importMessage(Message message) {
        try {
            if (!TransactionHelper.isTransactionActive()) {
                TransactionHelper.startTransaction();
                new Importer(mSession).importMessage(message);
                TransactionHelper.commitOrRollbackTransaction();
            } else {
                new Importer(mSession).importMessage(message);
            }
        } catch (DocumentNotFoundException e) {
            sLogger.error(e);
        }
    }

    public String getHash() {
        return mHash;
    }

    public void setHash(String hash) {
        mHash = hash;
    }

}
