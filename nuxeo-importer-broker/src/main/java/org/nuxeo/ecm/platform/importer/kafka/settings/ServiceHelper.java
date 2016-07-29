package org.nuxeo.ecm.platform.importer.kafka.settings;

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by anechaev on 7/25/16.
 * © Andrei Nechaev 2016
 */

public class ServiceHelper {
    public static Properties loadProperties(String name) throws IOException {
        InputStream is = Resources.getResource(name).openStream();
        Properties props = new Properties();
        props.load(is);

        return props;
    }
}
