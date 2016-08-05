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

package org.nuxeo.ecm.platform.importer.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.regex.Pattern;


public class Consumer<K, V> implements org.apache.kafka.clients.consumer.Consumer<K, V> {

    private KafkaConsumer<K, V> mConsumer;

    public Consumer(Properties properties) {
        this.mConsumer = new KafkaConsumer<>(properties);
    }

    @Override
    public Set<TopicPartition> assignment() {
        return mConsumer.assignment();
    }

    @Override
    public Set<String> subscription() {
        return mConsumer.subscription();
    }

    @Override
    public void subscribe(Collection<String> collection) {
        mConsumer.subscribe(collection);
    }

    @Override
    public void subscribe(Collection<String> collection, ConsumerRebalanceListener consumerRebalanceListener) {
        mConsumer.subscribe(collection, consumerRebalanceListener);
    }

    @Override
    public void assign(Collection<TopicPartition> collection) {
        mConsumer.assign(collection);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener consumerRebalanceListener) {
        mConsumer.subscribe(pattern, consumerRebalanceListener);
    }

    @Override
    public void unsubscribe() {
        mConsumer.unsubscribe();
    }

    @Override
    public ConsumerRecords<K, V> poll(long l) {
        return mConsumer.poll(l);
    }

    @Override
    public void commitSync() {
        mConsumer.commitSync();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> map) {
        mConsumer.commitSync();
    }

    @Override
    public void commitAsync() {
        mConsumer.commitSync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback offsetCommitCallback) {
        mConsumer.commitAsync(offsetCommitCallback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> map, OffsetCommitCallback offsetCommitCallback) {
        mConsumer.commitAsync(map, offsetCommitCallback);
    }

    @Override
    public void seek(TopicPartition topicPartition, long l) {
        mConsumer.seek(topicPartition, l);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> collection) {
        mConsumer.seekToBeginning(collection);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> collection) {
        mConsumer.seekToEnd(collection);
    }

    @Override
    public long position(TopicPartition topicPartition) {
        return mConsumer.position(topicPartition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition topicPartition) {
        return mConsumer.committed(topicPartition);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return mConsumer.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        return mConsumer.partitionsFor(s);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return mConsumer.listTopics();
    }

    @Override
    public Set<TopicPartition> paused() {
        return mConsumer.paused();
    }

    @Override
    public void pause(Collection<TopicPartition> collection) {
        mConsumer.pause(collection);
    }

    @Override
    public void resume(Collection<TopicPartition> collection) {
        mConsumer.resume(collection);
    }

    @Override
    public void close() {
        mConsumer.close();
    }

    @Override
    public void wakeup() {
        mConsumer.wakeup();
    }
}
