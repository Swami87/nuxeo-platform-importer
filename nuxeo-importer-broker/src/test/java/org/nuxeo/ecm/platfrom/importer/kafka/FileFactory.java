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

import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.impl.blob.StringBlob;
import org.nuxeo.ecm.core.blob.BlobManager;
import org.nuxeo.ecm.core.blob.SimpleManagedBlob;
import org.nuxeo.ecm.core.blob.binary.BinaryBlobProvider;
import org.nuxeo.ecm.platform.importer.kafka.message.Data;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.runtime.api.Framework;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

public class FileFactory {

    protected static List<Message> generateFileTree(int amount) {
        if (amount < 1) {
            throw new IllegalArgumentException("amount should be greater than 0");
        }

        List<Message> list = new ArrayList<>();
        Message message = generateMessage(1);
        list.add(message);
        list.addAll(generateTree(message, amount));

        return list;
    }


    private static List<Message> generateTree(Message message, Integer depth) {
        if (message == null || depth < 1) return new ArrayList<>();

        List<Message> list = new ArrayList<>();

        int rand = new Random().nextInt(depth) + 1;
        IntStream.range(0, rand).forEach(i -> {
            if (message.isFolderish()) {
                Message msg = generateMessage(depth * 100  + i);
                msg.setPath(Helper.getFullPath(message));
                msg.setParentHash(message.getHash());
                list.add(msg);
                list.addAll(generateTree(msg, depth - 1));
            }
        });


        return list;
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
        return new Data(blob);
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
