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

        Assert.assertTrue(m.getTitle().equals(mMessage.getTitle()));
    }
}
