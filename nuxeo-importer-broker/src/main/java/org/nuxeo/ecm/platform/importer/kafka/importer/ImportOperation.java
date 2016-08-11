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


import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.RecursiveAction;

public class ImportOperation extends RecursiveAction {

    private List<Message> mMessages = Collections.synchronizedList(new LinkedList<>());
    private DocumentModel mModel;

    public ImportOperation(DocumentModel model) {
        this.mModel = model;
    }

    @Override
    protected void compute() {
        Message currentMessage = mMessages.get(0);
        if (mMessages.size() > 1) {
            mMessages.remove(0);
            new ImportOperation(mModel).fork();
        }

        new Importer(mModel.getCoreSession(), currentMessage).runImport();
    }

    public void pushMessage(Message message) {
        mMessages.add(message);
    }

}
