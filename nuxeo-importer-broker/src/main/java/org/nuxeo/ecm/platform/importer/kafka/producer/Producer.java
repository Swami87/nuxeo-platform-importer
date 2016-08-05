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

package org.nuxeo.ecm.platform.importer.kafka.producer;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class Producer<K, V> implements org.apache.kafka.clients.producer.Producer<K, V> {

    private KafkaProducer<K, V> mProducer;

    public Producer(Properties properties) {
        this.mProducer = new KafkaProducer<>(properties);
    }


    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
        return mProducer.send(producerRecord);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
        return mProducer.send(producerRecord, callback);
    }

    @Override
    public void flush() {
        mProducer.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        return mProducer.partitionsFor(s);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return mProducer.metrics();
    }

    @Override
    public void close() {
        mProducer.close();
    }

    @Override
    public void close(long l, TimeUnit timeUnit) {
        mProducer.close(l, timeUnit);
    }


}
