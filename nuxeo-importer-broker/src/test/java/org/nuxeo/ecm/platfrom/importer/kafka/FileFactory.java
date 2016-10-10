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
import java.util.*;
import java.util.stream.IntStream;

public class FileFactory {

    public static Message generateRoot() {
        Message msg = FileFactory.generateMessage("/");
        msg.setFolderish(true);
        return msg;
    }

    public static List<Message> generateLevel(List<Message> previousLevel, Integer amount) {
        List<Message> level = new ArrayList<>(amount);

        IntStream.range(0, amount)
                .forEach(i -> previousLevel.stream()
                        .filter(Message::isFolderish)
                        .forEach(message -> {
                            String msgPath = Helper.getFullPath(message);
                            Message msg = generateMessage(msgPath);
                            level.add(msg);
                        }));

        return level;
    }

    private static Message generateMessage(String path) {
        int random  = new Random().nextInt(100);
        boolean isFolderish = random > 0;
        String type = isFolderish ? "Folder" : "File";
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        String title = String.valueOf(random) + "_" + type + "_" + uuid;
        Message msg = new Message();
        msg.setTitle(title);
        msg.setType(type);
        msg.setFolderish(isFolderish);
        msg.setPath(path);

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
