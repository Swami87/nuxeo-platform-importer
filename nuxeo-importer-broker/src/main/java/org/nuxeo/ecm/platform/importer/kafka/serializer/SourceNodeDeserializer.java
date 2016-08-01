package org.nuxeo.ecm.platform.importer.kafka.serializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.nuxeo.ecm.platform.importer.source.SourceNode;

import java.io.IOException;
import java.util.Map;

/**
 * Created by anechaev on 7/29/16.
 * © Andrei Nechaev 2016
 */
public class SourceNodeDeserializer implements Deserializer<SourceNode> {


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public SourceNode deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(bytes, SourceNode.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
