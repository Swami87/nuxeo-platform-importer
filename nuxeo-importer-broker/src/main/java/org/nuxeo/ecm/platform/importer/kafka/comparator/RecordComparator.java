package org.nuxeo.ecm.platform.importer.kafka.comparator;/*
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;

import java.util.Comparator;

public class RecordComparator implements Comparator<ConsumerRecord<String, Message>> {
    @Override
    public int compare(ConsumerRecord<String, Message> o1, ConsumerRecord<String, Message> o2) {
        Message first = o1.value();
        Message second = o2.value();

        return first.getPath().compareTo(second.getPath());
    }
}
