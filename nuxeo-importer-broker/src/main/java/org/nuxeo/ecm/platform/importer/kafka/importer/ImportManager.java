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


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.nuxeo.ecm.platform.importer.kafka.message.Message;
import org.nuxeo.ecm.platform.importer.kafka.operation.ImportOperation;
import org.nuxeo.ecm.platform.importer.kafka.operation.RecoveryOperation;
import org.nuxeo.ecm.platform.importer.kafka.settings.ServiceHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

public class ImportManager {

    private static final Log log = LogFactory.getLog(ImportManager.class);
    private static Boolean started = false;

    private Integer mThreads;
    private String mRepository;
    private Properties mConsumerProperties;
    private Integer mQueueSize;
    private Integer mBatchSize;

    private Set<Future<Integer>> mImportCallbacks = new HashSet<>();
    private Set<Future<Integer>> mRecoveryCallbacks = new HashSet<>();

    private ImportManager(Builder builder) throws IOException {
        mRepository = builder.mRepoName;
        mQueueSize = builder.mQueueSize;
        mThreads = builder.mThreads;
        Properties props;
        if (builder.mConsumerProps == null) {
            props = ServiceHelper.loadProperties("consumer.props");
            log.debug("Using default props for consumers");
        } else {
            props = builder.mConsumerProps;
        }
        mConsumerProperties = props;
        mBatchSize = builder.mBatchSize;
    }

    public Result syncImport(List<String> topics) throws IllegalStateException, InterruptedException, ExecutionException, TimeoutException {
        if (started) {
            throw new IllegalStateException("Manager already started");
        }

        started = true;

        for (String topic : topics) {
            final ThreadFactory factory = new ThreadFactoryBuilder()
                    .setNameFormat(topic + "-%d")
                    .setDaemon(false)
                    .build();
            ExecutorService pool = Executors.newFixedThreadPool(mThreads + 1, factory);

            final BlockingQueue<ConsumerRecord<String, Message>> recoveryQueue = new ArrayBlockingQueue<>(mQueueSize);
            IntStream.range(0, mThreads)
//                    .parallel() // Parallel should be used with bigger range, >1000, perhaps it's not our case
                    .forEach(i -> {
                        Callable<Integer> imOp = new ImportOperation(
                                mRepository,
                                Collections.singletonList(topic),
                                mConsumerProperties,
                                recoveryQueue,
                                mBatchSize
                        );
                        Future<Integer> imp = pool.submit(imOp);
                        mImportCallbacks.add(imp);
                    });

            Callable<Integer> reOp = new RecoveryOperation(recoveryQueue);
            mRecoveryCallbacks.add(pool.submit(reOp));

            pool.shutdown();
            pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
//            Thread.sleep(1500);
        }

        Integer imported = 0;
        for (Future<Integer> future : mImportCallbacks) {
            imported += future.get();
        }

        Integer recovered = 0;
        for (Future<Integer> future : mRecoveryCallbacks) {
            recovered += future.get();
        }

        started = false;
        return new Result(imported, recovered);
    }


    private Result waitUntilResult() throws InterruptedException, ExecutionException, TimeoutException {
        if (!started) {
            throw new IllegalStateException("Nothing to wait");
        }

        int imported = 0;
        for (Future<Integer> future : mImportCallbacks) {
            try {
                imported += future.get();
            } catch (Exception e) {// TODO: Understand why it throws "Not a folder"
                log.error(e);
            }
        }

        int recovered = 0;
        for (Future<Integer> f : mRecoveryCallbacks) {
            recovered += f.get();
        }

        return new Result(imported, recovered);
    }

    public static class Result {

        private Integer mImported;
        private Integer mRecovered;

        public Result(Integer imported, Integer recovered) {
            mImported = imported;
            mRecovered = recovered;
        }

        public Integer getImported() {
            return mImported;
        }

        public Integer getRecovered() {
            return mRecovered;
        }

        public void addResult(Result that) {
            mImported += that.getImported();
            mRecovered += that.getRecovered();
        }
    }


    public static class Builder {

        private String mRepoName;
        private Integer mThreads = 1;
        private Integer mQueueSize = 1000;
        private Integer mBatchSize = 50;
        private Properties mConsumerProps;

        public Builder(String repositoryName) {
            mRepoName = repositoryName;
        }

        public Builder threads(Integer num) {
            this.mThreads = num > 0 ? num : mThreads;
            return this;
        }

        public Builder queueSize(Integer num) {
            this.mQueueSize = num;
            return this;
        }

        public Builder batchSize(Integer num) {
            this.mBatchSize = num;
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
