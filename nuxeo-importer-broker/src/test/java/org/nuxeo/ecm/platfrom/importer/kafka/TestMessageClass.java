package org.nuxeo.ecm.platfrom.importer.kafka;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.kafka.serializer.MessageDeserializer;
import org.nuxeo.ecm.platform.importer.kafka.serializer.MessageSerializer;
import org.nuxeo.ecm.platform.importer.source.RandomTextSourceNode;
import org.nuxeo.ecm.platform.importer.source.SourceNode;

import java.io.IOException;
import java.util.Random;

/**
 * Created by anechaev on 8/3/16.
 * © Andrei Nechaev 2016
 */
public class TestMessageClass {

    private static SourceNode node = RandomTextSourceNode.init(1000, 20000, true);
    private Message mMessage;

    @Before
    public void setUp() throws IOException {
        Random random = new Random();
        int index = random.nextInt(node.getChildren().size());
        mMessage = new Message(node.getChildren().get(index));
    }

    @Test
    public void testShouldSerializeMessage() throws IOException {
        MessageSerializer serializer = new MessageSerializer();
        byte[] binaryMessage = serializer.serialize("", mMessage);

        System.out.println(new String(binaryMessage));

        MessageDeserializer deserializer = new MessageDeserializer();
        Message m = deserializer.deserialize("", binaryMessage);

        Assert.assertTrue(m.getName().equals(mMessage.getName()));
    }
}
