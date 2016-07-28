package org.nuxeo.ecm.platform.importer.service.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.service.*;
import org.nuxeo.ecm.platform.importer.service.kafka.broker.EventBroker;
import org.nuxeo.ecm.platform.importer.settings.Settings;
import org.nuxeo.runtime.model.ComponentContext;
import org.nuxeo.runtime.model.ComponentInstance;
import org.nuxeo.runtime.model.DefaultComponent;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by anechaev on 7/27/16.
 * © Andrei Nechaev 2016
 */
public class BrokerComponent extends DefaultComponent {

    private static final Log log = LogFactory.getLog(DefaultImporterComponent.class);
    public static final String BROKER_CONFIGURATION_XP = "brokerConfiguration";
    private Integer mPartition = 4;
    private Integer mReplication = 1;

    private EventBroker mBroker;
    private BrokerService mBrokerService;

    @Override
    public void registerContribution(Object contribution, String extensionPoint, ComponentInstance contributor) {
        if (!extensionPoint.equals(BROKER_CONFIGURATION_XP)) {
            return;
        }

        BrokerConfigurationDescriptor descriptor = (BrokerConfigurationDescriptor) contribution;
        Integer partition = descriptor.getPartition();
        if (partition != null) {
            mPartition = partition;
        }

        Integer replication = descriptor.getReplication();
        if (replication != null) {
            mReplication = replication;
        }
    }

    @Override
    public void activate(ComponentContext context) {
        Map<String, String> brokerProps = new HashMap<>();
        brokerProps.put(Settings.KAFKA, "kafka.props");
        brokerProps.put(Settings.ZOOKEEPER, "zk.props");

        try {
            mBroker = new EventBroker(brokerProps);
            mBroker.start();
            mBroker.createTopic(Settings.TASK, mPartition, mReplication);
            mBrokerService = new BrokerServiceImpl();
        } catch (Exception e) {
            log.error(e);
        }
    }

    @Override
    public void deactivate(ComponentContext context) {
        if (mBroker != null) {
            try {
                mBroker.stop();
            } catch (Exception e) {
                log.error(e);
            }
        }
        mBrokerService = null;
    }

    @Override
    public <T> T getAdapter(Class<T> adapter) {
        if (adapter.isAssignableFrom(BrokerService.class)) {
            return adapter.cast(mBrokerService);
        }
        return super.getAdapter(adapter);
    }
}
