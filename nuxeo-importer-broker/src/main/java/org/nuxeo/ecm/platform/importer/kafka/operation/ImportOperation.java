package org.nuxeo.ecm.platform.importer.kafka.operation;/*
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentNotFoundException;
import org.nuxeo.ecm.platform.importer.kafka.importer.Importer;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;

public class ImportOperation implements Operation {

    private static final Log log = LogFactory.getLog(ImportOperation.class);

    private CoreSession mCoreSession;

    public ImportOperation(CoreSession coreSession) {
        mCoreSession = coreSession;
    }

    @Override
    public boolean process(Message message) {
        try {
            new Importer(mCoreSession).importMessage(message);
            return true;
        } catch (DocumentNotFoundException e) {
            log.error(e);
            return false;
        }
    }
}
