/*
 * (C) Copyright 2006-2016 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * Contributors:
 *     Andrei Nechaev
 */

package org.nuxeo.ecm.platfrom.importer.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.importer.kafka.importer.ImportManager;
import org.nuxeo.ecm.platform.importer.kafka.message.Data;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
@Deploy({ "org.nuxeo.ecm.platform.filemanager.api", //
        "org.nuxeo.ecm.platform.filemanager.core", //
})
public class TestImportManager {
    private static final int DEPTH = 10;
    private static final int BLOBS_AMOUNT = 100;
    private static final Log sLogger = LogFactory.getLog(TestImportManager.class);

    private List<Message> mMessages = FileFactory.generateFileTree(DEPTH);

    @Inject
    CoreSession mCoreSession;

    @Before
    public void setUp() throws IOException {

        System.out.println(mMessages.size());
        Collections.reverse(mMessages);
        List<Data> blobs = new FileFactory().preImportBlobs(BLOBS_AMOUNT);

        Assert.assertEquals(BLOBS_AMOUNT, blobs.size());

        mMessages.stream()
                .parallel()
                .filter(message -> !message.isFolderish())
                .forEach(message -> {
                    int rand = new Random().nextInt(blobs.size());
                    Data data = blobs.get(rand);
                    message.setData(Collections.singletonList(data));
                });
    }

    @Test
    public void testShouldImportViaManager() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        ImportManager manager = new ImportManager(mCoreSession, 4);
        manager.pushAll(mMessages);
        manager.startImport();
        manager.waitUntilStop();

        long endTime = System.currentTimeMillis();

        System.out.println((endTime - startTime) / 1000.f + " sec");

        checkValues();
    }

    private void checkValues() {
        DocumentModelList list = mCoreSession.getChildren(mCoreSession.getRootDocument().getRef());
        List<DocumentModel> traversedList = Helper.traverse(list, mCoreSession);

        Assert.assertEquals(mMessages.size(), traversedList.size());
        Assert.assertTrue(mMessages.size() >= DEPTH);

        List<String> messagePaths = mMessages.stream()
                .map(Helper::getFullPath)
                .sorted()
                .collect(Collectors.toList());

        List<String> modelPaths = traversedList.stream()
                .map(DocumentModel::getPathAsString)
                .sorted()
                .collect(Collectors.toList());

        Assert.assertArrayEquals(messagePaths.toArray(), modelPaths.toArray());
    }

}
