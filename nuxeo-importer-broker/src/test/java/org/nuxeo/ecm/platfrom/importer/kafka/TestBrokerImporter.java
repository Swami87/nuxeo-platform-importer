package org.nuxeo.ecm.platfrom.importer.kafka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;

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
public class TestBrokerImporter {


    @Test
    public void testShouldCreatePartitionsAndReplications() throws IOException {
        Properties props = ServiceHelper.loadProperties("consumer.props");

    }
}
