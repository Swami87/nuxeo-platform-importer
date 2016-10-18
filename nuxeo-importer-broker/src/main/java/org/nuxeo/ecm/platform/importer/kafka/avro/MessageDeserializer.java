package org.nuxeo.ecm.platform.importer.kafka.avro;/*
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

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class MessageDeserializer implements Deserializer<Message> {

    private static final Log log = LogFactory.getLog(MessageDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Message deserialize(String topic, byte[] data) {
        DatumReader<Message> reader = new SpecificDatumReader<>(Message.class);
        Message message = null;
        try {
            message = reader.read(null, DecoderFactory.get().binaryDecoder(data, null));
        } catch (IOException e) {
            log.error(e);
        }
        return message;
    }

    @Override
    public void close() {

    }
}
