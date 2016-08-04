package org.nuxeo.ecm.platfrom.importer.kafka;

import org.nuxeo.ecm.platform.importer.source.SourceNode;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by anechaev on 8/4/16.
 * © Andrei Nechaev 2016
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
