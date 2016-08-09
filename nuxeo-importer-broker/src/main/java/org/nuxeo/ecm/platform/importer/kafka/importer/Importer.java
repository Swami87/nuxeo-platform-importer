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

package org.nuxeo.ecm.platform.importer.kafka.importer;

import org.nuxeo.ecm.core.api.Blobs;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.platform.importer.kafka.message.Data;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;


public class Importer {

    private DocumentModel mModel;
    private Message mMessage;

    public Importer(DocumentModel model, Message message) {
        this.mModel = model;
        this.mMessage = message;
    }

    public void runImport() {
//        UnrestrictedSessionRunner runner = new UnrestrictedSessionRunner(mModel.getRepositoryName()) {
//            @Override
//            public void run() {
                try {
                    processMessage(mModel.getCoreSession(), mMessage);
                } catch (IOException e) {
                    e.printStackTrace();
                }
//            }
//        };
//
//        runner.runUnrestricted();
    }


    private void processMessage(CoreSession session, Message message) throws IOException {
        if (message == null || session == null) return;

        String fileName = null;
        String name = null;

        List<Data> data = message.getData();
        if (data != null && data.size() > 0) {
            fileName = data.get(0).getFileName();
            Map<String, Serializable> props = message.getProperties();
            if (props != null) {
                name = (String) message.getProperties().get("name");
            }

            if (name == null) {
                name = fileName;
            } else if (fileName == null) {
                fileName = name;
            }

            DocumentModel doc = session.createDocumentModel(mModel.getPathAsString(), name, "File");

            doc.setProperty("dublincore", "title", name);
            doc.setProperty("file", "filename", fileName);
            Data d = message.getData().get(0);
            doc.setProperty("file", "content", Blobs.createBlob(d.getBytes(), d.getMimeType(), d.getEncoding()));

            if (props != null) {
                for (Map.Entry<String, Serializable> entry : props.entrySet()) {
                    doc.setPropertyValue(entry.getKey(), entry.getValue());
                }

                doc = session.saveDocument(doc);
            }

            session.createDocument(doc);
        }
    }
}
