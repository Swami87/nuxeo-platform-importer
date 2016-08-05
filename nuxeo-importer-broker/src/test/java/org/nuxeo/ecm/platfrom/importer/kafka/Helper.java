package org.nuxeo.ecm.platfrom.importer.kafka;

import org.nuxeo.ecm.platform.importer.source.SourceNode;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * (C) Copyright 2006-2016 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Andrei Nechaev
 */
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
}
