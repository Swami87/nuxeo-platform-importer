package org.nuxeo.ecm.platform.importer.tests.kafka_test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.importer.service.kafka.BrokerService;
import org.nuxeo.ecm.platform.importer.settings.ServiceHelper;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.LocalDeploy;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Created by anechaev on 7/28/16.
 * © Andrei Nechaev 2016
 */

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
@Deploy({ "org.nuxeo.ecm.platform.content.template",
        "org.nuxeo.ecm.platform.importer.core",
})
@LocalDeploy("org.nuxeo.ecm.platform.importer.core.test:test-broker-service-contrib.xml")
public class TestBrokerImporter {

    @Inject
    protected CoreSession mSession;

    @Inject
    protected BrokerService mBrokerService;

    @Test
    public void testShouldCreatePartitionsAndReplications() throws IOException {
        Properties props = ServiceHelper.loadProperties("consumer.props");
        mBrokerService.populateConsumers(props);


        assertEquals(8, mBrokerService.getPartition());
        assertEquals(2, mBrokerService.getReplication());
    }
}
