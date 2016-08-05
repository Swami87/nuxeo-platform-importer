package org.nuxeo.ecm.platform.importer.kafka.message;

import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.blobholder.BlobHolder;

import java.io.IOException;
import java.util.ArrayList;
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

public class DataFactory {

    public static List<Data> createData(BlobHolder holder) throws IOException {
        if (holder.getBlob() != null) {
            return new ArrayList<>(Collections.singleton(new Data(holder.getBlob())));
        } else if (holder.getBlobs() != null) {
            List<Data> list = new LinkedList<>();
            for (Blob b : holder.getBlobs()) {
                list.add(new Data(b));
            }
            return list;
        }
        return null;
    }
}
