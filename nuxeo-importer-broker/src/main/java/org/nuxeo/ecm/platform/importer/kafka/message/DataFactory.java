package org.nuxeo.ecm.platform.importer.kafka.message;

import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.blobholder.BlobHolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by anechaev on 8/3/16.
 * © Andrei Nechaev 2016
 */
public class DataFactory {

    public static List<Data> createData(BlobHolder holder) throws IOException {
        if (holder.getBlob() != null) {
            return new ArrayList<>(Collections.singleton(new Data(holder.getBlob())));
        } else if (holder.getBlobs() != null) {
            ArrayList<Data> list = new ArrayList<>(holder.getBlobs().size());
            for (Blob b : holder.getBlobs()) {
                list.add(new Data(b));
            }
            return list;
        }
        return null;
    }
}
