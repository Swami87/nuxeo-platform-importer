package org.nuxeo.ecm.platform.importer.service.kafka;

import org.nuxeo.common.xmap.annotation.XNode;
import org.nuxeo.common.xmap.annotation.XObject;

/**
 * Created by anechaev on 7/27/16.
 * © Andrei Nechaev 2016
 */

@XObject("brokerConfig")
public class BrokerConfigurationDescriptor {

    @XNode("@partition")
    protected Integer partition;

    @XNode("@replication")
    protected Integer replication;


    public Integer getPartition() {
        return partition;
    }

    public Integer getReplication() {
        return replication;
    }
}
