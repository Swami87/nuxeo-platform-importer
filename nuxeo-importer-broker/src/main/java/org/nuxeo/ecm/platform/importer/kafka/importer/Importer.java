package org.nuxeo.ecm.platform.importer.kafka.importer;

import org.nuxeo.ecm.core.api.Blobs;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.UnrestrictedSessionRunner;
import org.nuxeo.ecm.platform.importer.kafka.message.Data;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by anechaev on 8/2/16.
 * © Andrei Nechaev 2016
 */
public class Importer {

    private DocumentModel mModel;
    private Message mMessage;

    public Importer(DocumentModel model, Message message) {
        this.mModel = model;
        this.mMessage = message;
    }

    public void runImport() {
        UnrestrictedSessionRunner runner = new UnrestrictedSessionRunner(mModel.getRepositoryName()) {
            @Override
            public void run() {
                try {
                    processMessage(session, mMessage);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        runner.runUnrestricted();
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
