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
package org.nuxeo.ecm.platform.importer.kafka.consumer;

import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

public class ConsumerFactory {

    public static List<Consumer<String, Message>> createConsumers(Integer amount, String ...topics) throws IOException {
        Properties props = ServiceHelper.loadProperties("consumer.props");
        List<Consumer<String, Message>> consumers = Collections.synchronizedList(new ArrayList<>());
        IntStream.range(0, amount)
                .forEach(i -> {
                    Consumer<String, Message> consumer = new Consumer<>(props);
                    consumer.subscribe(Arrays.asList(topics));
                    consumers.add(consumer);
                });
        return consumers;
    }
}
