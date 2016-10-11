package org.nuxeo.ecm.platform.importer.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;

import javax.inject.Inject;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.common.utils.FileUtils;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.PathRef;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.importer.service.DefaultImporterService;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.LocalDeploy;
import org.nuxeo.runtime.transaction.TransactionHelper;
import org.nuxeo.ecm.platform.importer.tests.RepositoryInit;

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(init = RepositoryInit.class, cleanup = Granularity.METHOD)
@Deploy({ 
        "org.nuxeo.ecm.platform.importer.core", 
        "org.nuxeo.ecm.platform.filemanager.core",
        "org.nuxeo.ecm.platform.types.core",
        "org.nuxeo.ecm.platform.video.core",
        "org.nuxeo.ecm.platform.audio.core",
        "org.nuxeo.ecm.platform.picture.core"
})
@LocalDeploy({
        "org.nuxeo.ecm.platform.importer.core.test:test-importer-service-contrib3.xml" 
})
public class TestFileManagerDocumentModelFactory {

    @Inject
    protected CoreSession session;

    @Inject
    protected DefaultImporterService importerService;

    @Test
    public void testImporterContribution() throws Exception {

        File source = FileUtils.getResourceFileFromContext("test-data");
        importerService.importDocuments("/default-domain/workspaces/ws1", source.getPath(), false, 5, 5);
	session.save();

        DocumentModel file = session.getDocument(new PathRef("/default-domain/workspaces/ws1/test-data/sample.mpg"));
        assertNotNull(file);
        assertEquals("Video", file.getType());

        file = session.getDocument(new PathRef("/default-domain/workspaces/ws1/test-data/sample.wav"));
        assertNotNull(file);
        assertEquals("Audio", file.getType());

        file = session.getDocument(new PathRef("/default-domain/workspaces/ws1/test-data/exif_sample.jpg"));
        assertNotNull(file);
        assertEquals("Picture", file.getType());
        assertEquals("src2", file.getPropertyValue("dc:source"));
    }
}
