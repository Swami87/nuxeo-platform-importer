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
 *     anechaev
 */
package org.nuxeo.ecm.platform.importer.kafka.dispatcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MessageDispatcher {

    private static final Log log = LogFactory.getLog(MessageDispatcher.class);
    private ScheduledExecutorService mDispatchService = Executors.newSingleThreadScheduledExecutor();
    private Map<String, List<ConsumerRecord<String, Message>>> mQueueTable;

    public MessageDispatcher() {
        mQueueTable = Collections.synchronizedMap(new HashMap<>());

    }

    public void dispatch(ConsumerRecord<String, Message> record) {
        String path = record.value().getPath();
        String pathComponents[] = path.split("/");
        if (pathComponents.length > 1) {

            mQueueTable.entrySet()
                    .stream()
                    .filter(entry -> path.equals(entry.getKey()))
                    .forEach(entry -> {
                        entry.getValue().add(record);
                        if (record.value().isFolderish()) {
                            // TODO: Replace the key
                        }
                    });
        } else {

        }
    }

    protected void process(String queueName) {
        List<ConsumerRecord<String, Message>> queue = mQueueTable.get(queueName);
        Runnable task = new DispatchOperation(queue);
        mDispatchService.schedule(task, 5, TimeUnit.SECONDS);
    }

    private class DispatchOperation implements Runnable {

        private List<ConsumerRecord<String, Message>> mQueue;

        public DispatchOperation(List<ConsumerRecord<String, Message>> queue) {
            mQueue = queue;
        }

        @Override
        public void run() {
            for (ConsumerRecord<String, Message> record : mQueue) {
//                    new ImportOperation()
            }
        }
    }
}
