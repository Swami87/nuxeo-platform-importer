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

/**
 * (C) Copyright 2006-2016 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Andrei Nechaev
 */

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
