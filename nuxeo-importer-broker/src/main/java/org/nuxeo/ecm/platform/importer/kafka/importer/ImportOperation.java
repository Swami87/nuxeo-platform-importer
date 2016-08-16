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


import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentNotFoundException;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.runtime.transaction.TransactionHelper;

import java.util.*;
import java.util.concurrent.RecursiveAction;

public class ImportOperation extends RecursiveAction {

    private List<Message> mMessages = Collections.synchronizedList(new LinkedList<>());
    private Set<String> hashes;
    private DocumentModel mModel;

    public ImportOperation(DocumentModel model, Set<String> set) {
        this.mModel = model;
        this.hashes = set;
    }

    @Override
    protected void compute() {
        Message message = findMessage();

        if (mMessages.size() == 0 || message == null) return;

        if (TransactionHelper.isTransactionActive()) {
            process(message);
        } else {
            TransactionHelper.startTransaction();
            process(message);
            TransactionHelper.commitOrRollbackTransaction();
        }

        mMessages.remove(message);

        ImportOperation operation = new ImportOperation(mModel, hashes);
        operation.pushAll(mMessages);
        operation.fork();

    }

    private Message findMessage() {
        for (Message message : mMessages) {
            if (message.getParentHash() == null ||
                    (message.getParentHash() != null && hashes.contains(message.getParentHash()))) {
                return message;
            }
        }

        return null;
    }

    private void process(Message message) {
        try {
            new Importer(mModel.getCoreSession()).importMessage(message);
            hashes.add(message.getHash());
//            System.out.println("Success: " + message.getPath() + ", folder ?: " + message.isFolderish());
        } catch (DocumentNotFoundException e) {
            System.out.println("Couldn't find the " + e.getLocalizedMessage());
        }

    }

    public void pushMessage(Message message) {
        mMessages.add(message);
    }

    private void pushAll(Collection<Message> messages) {
        mMessages.addAll(messages);
    }

    public int count() {
        return mMessages.size();
    }

}
