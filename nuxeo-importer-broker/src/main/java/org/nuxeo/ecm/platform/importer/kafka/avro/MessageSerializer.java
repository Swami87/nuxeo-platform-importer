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
package org.nuxeo.ecm.platform.importer.kafka.avro;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class MessageSerializer implements Serializer<Message> {

    private static final Log log = LogFactory.getLog(MessageSerializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Message data) {
        DatumWriter<Message> messageDatumWriter = new SpecificDatumWriter<>(Message.class);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
            messageDatumWriter.write(data, encoder);
            encoder.flush();

            return baos.toByteArray();
        } catch (IOException e) {
            log.error(e);
        }

        return new byte[0];
    }

    @Override
    public void close() {

    }
}
