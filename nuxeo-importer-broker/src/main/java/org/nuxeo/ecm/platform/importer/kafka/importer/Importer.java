package org.nuxeo.ecm.platform.importer.kafka.importer;

import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.UnrestrictedSessionRunner;
import org.nuxeo.ecm.core.api.blobholder.BlobHolder;
import org.nuxeo.ecm.core.api.model.PropertyNotFoundException;
import org.nuxeo.ecm.platform.filemanager.api.FileManager;
import org.nuxeo.ecm.platform.importer.source.SourceNode;
import org.nuxeo.runtime.api.Framework;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Created by anechaev on 8/2/16.
 * © Andrei Nechaev 2016
 */
public class Importer {

    private final FileManager fileManager;

    private DocumentModel mModel;
    private SourceNode mNode;

    public Importer(DocumentModel mModel, SourceNode mNode) {
        this.fileManager = Framework.getService(FileManager.class);
        this.mModel = mModel;
        this.mNode = mNode;
    }

    public void runImport() {
        UnrestrictedSessionRunner runner = new UnrestrictedSessionRunner(mModel.getRepositoryName()) {
            @Override
            public void run() {
                try {
                    processBlob(session, mNode);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        runner.runUnrestricted();
    }

    private void processBlob(CoreSession session, SourceNode node) throws IOException {
        String fileName = null;
        String name = null;
        BlobHolder blobHolder = node.getBlobHolder();
        if (blobHolder != null) {
            Blob blob = blobHolder.getBlob();
            if (blob != null) {
                fileName = blob.getFilename();
            }
            Map<String, Serializable> props = blobHolder.getProperties();
            if (props != null) {
                name = (String) props.get("name");
            }
            if (name == null) {
                name = fileName;
            } else if (fileName == null) {
                fileName = name;
            }

            DocumentModel doc = session.createDocumentModel(mModel.getPathAsString(), name, "File");

            doc.setProperty("dublincore", "title", name);
            doc.setProperty("file", "filename", fileName);
            doc.setProperty("file", "content", blobHolder.getBlob());

            if (props != null) {

                for (Map.Entry<String, Serializable> entry : props.entrySet()) {
                    try {
                        doc.setPropertyValue(entry.getKey(), entry.getValue());
                    } catch (PropertyNotFoundException e) {
                        String message = String.format("Property '%s' not found on document type: %s. Skipping it.",
                                entry.getKey(), doc.getType());
                        System.out.println(message);
                        e.printStackTrace();
                    }
                }
                doc = session.saveDocument(doc);
            }

            session.createDocument(doc);
        }
    }
}
