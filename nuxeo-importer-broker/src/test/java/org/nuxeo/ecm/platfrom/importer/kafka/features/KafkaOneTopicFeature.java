package org.nuxeo.ecm.platfrom.importer.kafka.features;/*
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
 *     anechaev
 */

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.ZkUtils;

import java.util.List;
import java.util.Properties;

public class KafkaOneTopicFeature extends AbstractKafkaFeature {

    public static final String TOPIC = "test";

    @Override
    public void propagateTopics(ZkUtils utils, List<KafkaServer> servers, Integer replications, Integer partitions) {
        AdminUtils.createTopic(utils, TOPIC, partitions, replications, new Properties(),
                RackAwareMode.Disabled$.MODULE$);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), TOPIC, 0, 10000);
    }
}
