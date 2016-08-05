package org.nuxeo.ecm.platform.importer.kafka.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;

import java.io.IOException;
import java.util.Map;

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

public class MessageSerializer implements Serializer<Message> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Message message) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsBytes(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
