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
 *     anechaev
 */


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.kafka.producer.Producer;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class RecoveryOperation implements Callable<Integer> {

    private static final Log log = LogFactory.getLog(RecoveryOperation.class);
    private BlockingQueue<ConsumerRecord<String, Message>> mRecoveryQueue;

    public RecoveryOperation(BlockingQueue<ConsumerRecord<String, Message>> queue) {
        mRecoveryQueue = queue;
    }

    @Override
    public Integer call() throws Exception {
        Integer recovered = 0;
        try (Producer<String, Message> producer = new Producer<>(ServiceHelper.loadProperties("producer.props"))){
            ConsumerRecord<String, Message> record;
            while ((record = mRecoveryQueue.poll(60, TimeUnit.SECONDS)) != null) {
                if (record.key().equals("POISON")) break;
                producer.send(new ProducerRecord<>(
                        record.topic(),
                        record.partition(),
                        record.key(),
                        record.value()));
                recovered++;
            }
        } catch (IOException e) {
            log.error(e);
        }

        return recovered;
    }
}
