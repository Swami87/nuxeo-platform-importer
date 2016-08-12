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

import org.nuxeo.ecm.core.api.*;
import org.nuxeo.ecm.core.blob.BlobManager;
import org.nuxeo.ecm.core.blob.SimpleManagedBlob;
import org.nuxeo.ecm.platform.importer.kafka.message.Data;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

public class FileFactory {

    private CoreSession mSession;

    FileFactory(CoreSession session) {
        this.mSession = session;
    }

    static List<Message> generateFileTree(int amount) {
        if (amount < 1) {
            throw new IllegalArgumentException("amount should be greater than 0");
        }

        List<Message> list = new LinkedList<>();

        for (int i = 1; i <= amount; i++) {
            Message message = generateMessage(i);
            list.add(message);
            if (message.isFolderish()) {
                for (int j = 1; j <= amount; j++) {
                    Message nestedMessage = generateMessage(i*100 + j);
                    String pathSeparator = message.getPath().equals("/") ? "" : "/";
                    nestedMessage.setPath(message.getPath() + pathSeparator + message.getTitle());
                    nestedMessage.setParentHash(message.getHash());
                    list.add(nestedMessage);
                }
            }
        }

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

    static Data generateData(String digest, long length) throws IOException {
        BlobManager.BlobInfo info = new BlobManager.BlobInfo();
        info.digest = digest;
        info.mimeType = "plain/text";
        info.filename = UUID.randomUUID().toString() + ".txt";
        info.encoding = "UTF-8";
        info.length = length;

        Blob blob = new SimpleManagedBlob(info);
        return new Data(blob);
    }

    protected List<Blob> preImportBlobs(int amount) throws IllegalArgumentException {
        if (amount < 1) {
            throw new IllegalArgumentException("amount should be greater than 0");
        }

        List<Blob> blobs = new LinkedList<>();
        IntStream.range(0, amount)
                .forEach( i -> {
                    String randomName = UUID.randomUUID().toString();
                    DocumentModel created = createFileDocument(randomName);

                    DocumentModel model = mSession.getDocument(new PathRef(created.getPathAsString()));
                    Blob b = (Blob) model.getProperty("file", "content");

                    blobs.add(b);
                });

        return blobs;
    }


    protected DocumentModel createFileDocument(String filename) {
        DocumentModel fileDoc = mSession.createDocumentModel("/", filename, "File");
        fileDoc.setProperty("dublincore", "title", filename.toUpperCase());

        Blob blob = createBlob(filename);
        fileDoc.setProperty("file", "content", blob);

        fileDoc = mSession.createDocument(fileDoc);
        return fileDoc;
    }



    private Blob createBlob(String data) {
        Blob blob = Blobs.createBlob(data.getBytes());
        blob.setFilename(data + ".txt");
        blob.setMimeType("plain/text");

        return blob;
    }
}
