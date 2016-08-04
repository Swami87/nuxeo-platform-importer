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
                List<SourceNode> tmpList = new LinkedList<>(node.getChildren());
                list.addAll(traverseList(tmpList));
            }
        }

        return list;
    }

    protected static List<SourceNode> traverse(SourceNode root) throws IOException {
        List<SourceNode> list = new LinkedList<>(Collections.singleton(root));

        return traverseList(list);
    }
}
