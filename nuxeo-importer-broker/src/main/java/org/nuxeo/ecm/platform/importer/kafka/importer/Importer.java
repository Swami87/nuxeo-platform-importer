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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.blob.BlobManager;
import org.nuxeo.ecm.core.blob.SimpleManagedBlob;
import org.nuxeo.ecm.platform.importer.kafka.message.Data;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.runtime.api.Framework;

import java.io.Serializable;

public class Importer {

    private static final Log log = LogFactory.getLog(Importer.class);

    private CoreSession mCoreSession;

    public Importer(CoreSession session) {
        this.mCoreSession = session;
    }


    public void importMessage(Message message) throws NuxeoException {
        DocumentModel model = mCoreSession.createDocumentModel(message.getPath(), message.getTitle(), message.getType());
        model.setProperty("dublincore", "title", model.getTitle());

        if (message.getData() != null && model.hasSchema("file")) {
            int index = 0;
            for (Data data : message.getData()) {
                Blob blob = createBlobWith(data);
                model.setPropertyValue(data.getDataPaths().get(index), (Serializable)blob);
                index++;
            }
        }

        mCoreSession.createDocument(model);
        log.debug("Imported: " + message);
    }

    private Blob createBlobWith(Data data) {
        BlobManager blobManager = Framework.getService(BlobManager.class);
        String provider = blobManager.getBlobProviders().keySet().iterator().next();

        BlobManager.BlobInfo info = new BlobManager.BlobInfo();

        info.key = provider + ":" + data.getDigest();
        info.digest = data.getDigest();
        info.mimeType = data.getMimeType();
        info.filename = data.getFileName();
        info.encoding = data.getEncoding();
        info.length = data.getLength();

        return new SimpleManagedBlob(info);
    }
}
