package org.nuxeo.ecm.platform.importer.kafka.serializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.nuxeo.ecm.platform.importer.source.SourceNode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
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

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try (ObjectInput in = new ObjectInputStream(bis);) {
            return (SourceNode) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                bis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    @Override
    public void close() {

    }
}
