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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.kafka.operation.DefaultImportOperation;
import org.nuxeo.ecm.platform.importer.kafka.operation.ImportOperation;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ImportManager {

    private static final Log log = LogFactory.getLog(ImportManager.class);

    private BlockingQueue<Message> mQueue = new LinkedBlockingQueue<>();

    // TODO: cannot use recursive tasks, since amount of consumers exceeds expectations. => use a thread pool instead
    private ForkJoinPool mPool;
    private CoreSession mSession;
    private Boolean started = false;

    public ImportManager(CoreSession session, Integer threadNum) {
        this.mPool = new ForkJoinPool(threadNum);
        this.mSession = session;
    }

    public void start(Integer consumers, String ...topics) throws Exception {
        if (started) {
            throw new Exception("Manager already started");
        }
        started = true;

        for (int i = 0; i < consumers; i++) {
            ImportOperation operation = new ImportOperation(mSession, Arrays.asList(topics));
            mPool.invoke(operation);
        }
    }


    public void startImport() throws InterruptedException {
        if (!mPool.isShutdown()) {
            DefaultImportOperation operation = new DefaultImportOperation(mSession, mQueue);
            mPool.invoke(operation);
        }
    }


    public void waitUntilStop() throws InterruptedException {
        mPool.shutdown();
        mPool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        started = false;
    }

    public void push(Message message) {
        mQueue.add(message);
    }

    public void pushAll(Collection<Message> messages) {
        mQueue.addAll(messages);
    }
}
