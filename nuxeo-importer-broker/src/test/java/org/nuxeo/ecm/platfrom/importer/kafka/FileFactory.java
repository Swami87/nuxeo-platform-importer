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

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

public class FileFactory {

    private CoreSession mSession;

    FileFactory(CoreSession session) {
        this.mSession = session;
    }

    protected List<String> generateFiles(int amount) throws IllegalArgumentException {
        if (amount < 1) {
            throw new IllegalArgumentException("amount should be greater than 0");
        }

        List<String> digests = new LinkedList<>();
        IntStream.range(0, amount)
                .forEach( i -> {
                    String randomName = UUID.randomUUID().toString();
                    DocumentModel created = createFileDocument(randomName);

                    DocumentModel model = mSession.getDocument(new PathRef(created.getPathAsString()));
                    Blob b = (Blob) model.getProperty("file", "content");
                    digests.add(b.getDigest());
                });

        return digests;
    }

    protected DocumentModel createFileDocument(String filename) {
        DocumentModel fileDoc = mSession.createDocumentModel("/", filename, "File");
        fileDoc.setProperty("dublincore", "title", filename.toUpperCase());

        Blob blob = Blobs.createBlob(filename.getBytes());
        blob.setFilename("test.txt");
        blob.setMimeType("plain/text");
        fileDoc.setProperty("file", "content", blob);

        fileDoc = mSession.createDocument(fileDoc);
        return fileDoc;
    }
}
