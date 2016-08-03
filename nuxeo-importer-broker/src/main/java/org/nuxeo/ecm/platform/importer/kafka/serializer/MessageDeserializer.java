package org.nuxeo.ecm.platform.importer.kafka.serializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;

import java.io.IOException;
import java.util.Map;

/**
 * Created by anechaev on 8/3/16.
 * © Andrei Nechaev 2016
 */
public class MessageDeserializer implements Deserializer<Message> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Message deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(bytes, Message.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
