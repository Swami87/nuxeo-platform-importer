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
import java.util.List;

public class RecoveryOperation implements Operation {

    private static final Log log = LogFactory.getLog(RecoveryOperation.class);
    private List<ConsumerRecord<String, Message>> mRecords;

    public RecoveryOperation(List<ConsumerRecord<String, Message>> list) {
        mRecords = list;
    }

    @Override
    public Integer call() throws Exception {
        try (Producer<String, Message> producer = new Producer<>(ServiceHelper.loadProperties("producer.props"))){
            mRecords.forEach(record -> producer.send(new ProducerRecord<>(
                    record.topic(),
                    record.partition(),
                    record.key(),
                    record.value()
            )));
        } catch (IOException e) {
            log.error(e);
            return 0;
        }

        return 1;
    }

    @Override
    public boolean process(Message message) {
        return false;
    }
}
