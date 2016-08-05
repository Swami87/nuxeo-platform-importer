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
 * (C) Copyright 2006-2016 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Andrei Nechaev
 */

public class TestMessageClass {

    private static SourceNode node = RandomTextSourceNode.init(1000, 512, true);
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
