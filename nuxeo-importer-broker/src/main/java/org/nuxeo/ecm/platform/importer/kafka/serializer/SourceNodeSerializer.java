package org.nuxeo.ecm.platform.importer.kafka.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.nuxeo.ecm.platform.importer.source.SourceNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;


/**
 * Created by anechaev on 7/29/16.
 * © Andrei Nechaev 2016
 */
public class SourceNodeSerializer implements Serializer<SourceNode> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, SourceNode sourceNode) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(sourceNode);

            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        return new byte[0];
    }

    @Override
    public void close() {

    }
}
