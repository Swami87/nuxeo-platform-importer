/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
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
 */
package org.nuxeo.ecm.platform.importer.queue.consumer;

import org.nuxeo.ecm.platform.importer.queue.TaskRunner;


/**
 * @since 8.3
 */
public interface Consumer extends TaskRunner {

    double getNbDocsCreated();

    /**
     * @deprecated  Stats are now available using metrics
     */
    @Deprecated
    double getImmediateThroughput();

    /**
     * @deprecated  Stats are now available using metrics
     */
    @Deprecated
    double getThroughput();

    /**
     * @deprecated  Stats are now available using metrics
     */
    @Deprecated
    ImportStat getImportStat();
}
