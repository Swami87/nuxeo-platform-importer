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
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.transaction.TransactionHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

public class FileFactory {

    private CoreSession mSession;

    FileFactory(CoreSession session) {
        this.mSession = session;
    }

    static Message createMessage(DocumentModel model) throws IOException {
        Message msg = new Message();
        msg.setTitle(model.getName());
        msg.setFolderish(model.isFolder());
        msg.setPath(model.getPathAsString());

        Blob blob = (Blob) model.getProperty("file", "content");
        Data data = new Data(blob);
        msg.setData(Collections.singletonList(data));

        return msg;
    }

    static Message generateMessage() {
        Message msg = new Message();

        String title = UUID.randomUUID().toString();
        msg.setTitle(title.toLowerCase());
        msg.setHash(title);
        msg.setFolderish(false);
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

    // TODO: Make it random make it crazy :)
    protected List<Message> createDocumentsOnServer(int amount) throws IOException {
        List<Message> list = new LinkedList<>();

        for (int i = 0; i < amount; i++) {
            String docName = UUID.randomUUID().toString();
            createFileDocument(docName);

            DocumentModel model = mSession.getDocument(new PathRef("/" + docName));

            list.add(createMessage(model));
        }

        return list;
    }

    protected DocumentModel createFileDocument(String filename) {
        DocumentModel fileDoc = mSession.createDocumentModel("/", filename, "File");
        fileDoc.setProperty("dublincore", "title", filename.toUpperCase());

        Blob blob = createBlob(filename);
        fileDoc.setProperty("file", "content", blob);

        fileDoc = mSession.createDocument(fileDoc);
        return fileDoc;
    }

    protected DocumentModel createFileDocument(Message message) {
        DocumentModel fileDoc = mSession.createDocumentModel("/", message.getTitle(), "File");
        fileDoc.setProperty("dublincore", "title", message.getTitle().toUpperCase());

        if (message.getData() != null) {
            BlobManager manager = Framework.getService(BlobManager.class);
            String providerId = manager.getBlobProviders().keySet().iterator().next();

            Data data = message.getData().get(0);
            BlobManager.BlobInfo info = new BlobManager.BlobInfo();

            info.key = providerId + ":" + data.getDigest();
            info.digest = message.getData().get(0).getDigest();
            info.mimeType = data.getMimeType();
            info.filename = data.getFileName();
            info.encoding = data.getEncoding();
            info.length = data.getLength();

            Blob blob = new SimpleManagedBlob(info);
            fileDoc.setProperty("file", "content", blob);
        }


        TransactionHelper.startTransaction();
        fileDoc = mSession.createDocument(fileDoc);
        TransactionHelper.commitOrRollbackTransaction();
        return fileDoc;
    }



    private Blob createBlob(String data) {
        Blob blob = Blobs.createBlob(data.getBytes());
        blob.setFilename(data + ".txt");
        blob.setMimeType("plain/text");

        return blob;
    }
}
