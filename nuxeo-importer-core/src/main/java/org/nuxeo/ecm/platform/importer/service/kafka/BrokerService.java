package org.nuxeo.ecm.platform.importer.service.kafka;

import java.util.Properties;

/**
 * Created by anechaev on 7/27/16.
 * © Andrei Nechaev 2016
 */
public interface BrokerService {

    void populateConsumers(Properties props);

    void terminateServicePool();

    int getPartition();

    void setPartition(int partition);

    int getReplication();

    void setReplication(int replication);
}
