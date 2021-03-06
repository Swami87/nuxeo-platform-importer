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
package org.nuxeo.ecm.platform.importer.queue.producer;

import java.util.List;

import org.nuxeo.ecm.platform.importer.log.ImporterLogger;
import org.nuxeo.ecm.platform.importer.source.SourceNode;

/**
 * @since 8.3
 */
public class SourceNodeProducer extends AbstractProducer {

    protected final SourceNode root;

    public SourceNodeProducer(SourceNode root, ImporterLogger log) {
        super(log);
        this.root = root;
    }

    @Override
    public void run() {
        try {
            started = true;
            submit(root);
            completed = true;
        } catch (Exception e) {
            log.error("Error during sourceNode processing", e);
            error = e;
        }
    }

    protected void submit(SourceNode node) throws Exception {
        if (node == null) {
            return;
        }
        dispatch(node);
        List<SourceNode> children = node.getChildren();
        if (children != null) {
            for (SourceNode child : children) {
                submit(child);
            }
        }
    }

}
