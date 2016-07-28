package org.nuxeo.ecm.platform.importer.tests.kafka_test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.platform.importer.service.kafka.BrokerService;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

/**
 * Created by anechaev on 7/28/16.
 * © Andrei Nechaev 2016
 */

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@Deploy({ "org.nuxeo.ecm.core.api", "org.nuxeo.ecm.core", "org.nuxeo.ecm.core.schema",
        "org.nuxeo.ecm.platform.importer.core:OSGI-INF/broker-service.xml" })
public class TestDeployment {

    BrokerService mService;

    @After
    public void down() {
        mService.terminateServicePool();
    }
    @Test
    public void testImport() {
        mService= Framework.getService(BrokerService.class);

        assertNotNull(mService);
    }
}
