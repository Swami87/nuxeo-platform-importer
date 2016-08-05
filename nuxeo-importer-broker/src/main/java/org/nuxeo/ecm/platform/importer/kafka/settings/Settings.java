package org.nuxeo.ecm.platform.importer.kafka.settings;

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

public class Settings {
    public static final String ZOOKEEPER = "zk_props";
    public static final String KAFKA = "kafka_props";

    public static final int CONNECTION_TIMEOUT = 30_000;
    public static final int SESSION_TIMEOUT = 30_000;

    public static final String TASK = "task";
    public static final Integer DEFAULT_PARTITION = 4;
    public static final Integer DEFAULT_REPLICATION = 1;
}
