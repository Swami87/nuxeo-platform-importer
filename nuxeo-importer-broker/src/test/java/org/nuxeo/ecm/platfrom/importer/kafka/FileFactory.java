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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.impl.blob.StringBlob;
import org.nuxeo.ecm.core.blob.BlobManager;
import org.nuxeo.ecm.core.blob.SimpleManagedBlob;
import org.nuxeo.ecm.core.blob.binary.BinaryBlobProvider;
import org.nuxeo.ecm.platform.importer.kafka.message.Data;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.kafka.producer.Producer;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;
import org.nuxeo.runtime.api.Framework;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class FileFactory {

    public static AtomicInteger counter = new AtomicInteger(0);

    public static void generateTree(List<Data> data, String topic, Integer depth) {
        if (depth < 1) return;

        Message message = FileFactory.generateMessage(1);
        try (Producer<String, Message> producer = new Producer<>(ServiceHelper.loadProperties("producer.props"))) {
            IntStream.range(0, depth)
//                    .parallel()
                    .forEach(i -> {
                        message.setFolderish(true);
                        message.setType("Folder");
                        producer.send(new ProducerRecord<>(topic, "Msg", message));
                        producer.flush();
                        counter.incrementAndGet();
                        send(producer, topic, message, data, depth);
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void send(Producer<String, Message> producer, String topic, Message message, List<Data> data, Integer depth) {
        if (depth <= 0) return;
        counter.incrementAndGet();
        Message msg = generateMessage(depth * 100  + (depth % 100));
        msg.setPath(Helper.getFullPath(message));
        msg.setParentHash(message.getHash());

        if (!msg.isFolderish()) {
            int rand = new Random().nextInt(data.size());
            Data bin = data.get(rand);
            message.setData(Collections.singletonList(bin));
        } else {
            send(producer, topic, msg, data, depth-1);
        }

        producer.send(new ProducerRecord<>(topic, "Key", msg));
        producer.flush();
    }


    private static Message generateMessage(int num) {
        int random  = new Random(num).nextInt(100);
        boolean isFolderish = random > 50;
        String type = isFolderish ? "Folder" : "File";
        String title = String.valueOf(num) + "_" + type;

        Message msg = new Message();
        msg.setTitle(title);
        msg.setType(type);
        msg.setFolderish(isFolderish);
        msg.setPath("/");

        return msg;
    }


    private static Data generateData(String digest, long length) throws IOException {
        BlobManager.BlobInfo info = new BlobManager.BlobInfo();
        info.digest = digest;
        info.mimeType = "plain/text";
        info.filename = UUID.randomUUID().toString() + ".txt";
        info.encoding = "UTF-8";
        info.length = length;

        Blob blob = new SimpleManagedBlob(info);
        Data data = new Data(blob);
        data.setDataPaths(Collections.singletonList("file:content"));
        return data;
    }


    protected List<Data> preImportBlobs(int amount) throws IllegalArgumentException {
        if (amount < 1) {
            throw new IllegalArgumentException("amount should be greater than 0");
        }

        List<Data> info = new ArrayList<>(amount);

        BlobManager manager = Framework.getService(BlobManager.class);
        BinaryBlobProvider provider = (BinaryBlobProvider)manager.getBlobProvider("test");

        IntStream.range(0, amount)
                .forEach( i -> {
                    String textData = UUID.randomUUID().toString();

                    Blob blob = new StringBlob(textData);
                    blob.setFilename("blob_" + i + ".txt");
                    blob.setMimeType("plain/text");
                    try {
                        String digest = provider.writeBlob(blob, null);
                        Data data = generateData(digest, blob.getLength());
                        info.add(data);
                    } catch (IOException e) {
                        System.out.println(e.getLocalizedMessage());
                    }
                });

        return info;
    }
}
