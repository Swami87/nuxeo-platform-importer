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

import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.source.SourceNode;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


public class Helper {

    protected static List<SourceNode> traverseList(List<SourceNode> nodes) throws IOException {
        List<SourceNode> list = new LinkedList<>(nodes);
        for (SourceNode node : nodes) {
            if (node.getChildren() != null) {
                list.addAll(traverseList(node.getChildren()));
            }
        }

        return list;
    }


    protected static List<SourceNode> traverse(SourceNode root) throws IOException {
        List<SourceNode> list = new LinkedList<>(Collections.singletonList(root));
        return traverseList(list);
    }

    protected static List<DocumentModel> traverse(DocumentModelList modelList, CoreSession session) {
        List<DocumentModel> list = new LinkedList<>(modelList);
        for (DocumentModel model : modelList) {
            DocumentModelList children = session.getChildren(model.getRef());
            if (children != null && children.size() > 0) {
                list.addAll(traverse(children, session));
            }
        }

        return list;
    }

    protected static String getSeparator(Message message) {
        return message.getPath()
                .substring(message.getPath().length()-1)
                .equalsIgnoreCase("/") ? "" : "/";
    }
}
