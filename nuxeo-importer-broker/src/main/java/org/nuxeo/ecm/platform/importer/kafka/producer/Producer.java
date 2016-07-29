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
 * Created by anechaev on 7/25/16.
 * © Andrei Nechaev 2016
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
