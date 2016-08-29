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
package org.nuxeo.ecm.platform.importer.kafka.importer;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.kafka.operation.ImportOperation;
import org.nuxeo.ecm.platform.importer.kafka.operation.RecoveryOperation;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

public class ImportManager {

    private static final Log log = LogFactory.getLog(ImportManager.class);
    private static Boolean started = false;

    private String mRepository;
    private ForkJoinPool mPool;
    private Properties mConsumerProperties;

    private List<Future<Integer>> mCallbacks = new ArrayList<>();
    private BlockingQueue<ConsumerRecord<String, Message>> mRecoveryQueue;

    private ImportManager(Builder builder) throws IOException {
        mPool = new ForkJoinPool(builder.mThreads + 1);
        mRepository = builder.mRepoName;
        mRecoveryQueue = new ArrayBlockingQueue<>(builder.mQueueSize);

        Properties props;
        if (builder.mConsumerProps == null) {
            props = ServiceHelper.loadProperties("consumer.props");
        } else {
            props = builder.mConsumerProps;
        }
        mConsumerProperties = props;
    }

    public void start(Integer consumers, String ...topics) throws Exception {
        if (started) {
            throw new Exception("Manager already started");
        }
        started = true;

        for (int i = 0; i < consumers; i++) {
            ImportOperation operation = new ImportOperation(mRepository, Arrays.asList(topics), mConsumerProperties, mRecoveryQueue);
            mCallbacks.add(mPool.submit(operation));
        }

        RecoveryOperation operation = new RecoveryOperation(mRecoveryQueue);
        mPool.submit(operation);
    }


    public int waitUntilStop() throws InterruptedException, ExecutionException {
        mPool.shutdown();
        mPool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        started = false;

        int count = 0;
        for (Future<Integer> f : mCallbacks) {
            count += f.get();
        }
        return count;
    }


    public static class Builder {

        private String mRepoName;
        private Integer mThreads = 1;
        private Integer mQueueSize = 1000;
        private Properties mConsumerProps;

        public Builder(String repositoryName) {
            mRepoName = repositoryName;
        }

        public Builder threads(Integer num) {
            this.mThreads = num;
            return this;
        }

        public Builder queueSize(Integer num) {
            this.mQueueSize = num;
            return this;
        }

        public Builder consumer(Properties props) {
            this.mConsumerProps = props;
            return this;
        }

        public ImportManager build() throws IOException {
            return new ImportManager(this);
        }
    }
}
