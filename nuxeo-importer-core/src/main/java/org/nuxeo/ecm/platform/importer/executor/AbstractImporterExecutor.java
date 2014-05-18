/*
 * (C) Copyright 2006-2008 Nuxeo SAS (http://nuxeo.com/) and contributors.
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
 *     Nuxeo - initial API and implementation
 *
 * $Id$
 */

package org.nuxeo.ecm.platform.importer.executor;

import org.apache.commons.logging.Log;
import org.nuxeo.ecm.platform.importer.base.ImporterRunner;
import org.nuxeo.ecm.platform.importer.factories.DefaultDocumentModelFactory;
import org.nuxeo.ecm.platform.importer.factories.ImporterDocumentModelFactory;
import org.nuxeo.ecm.platform.importer.log.BasicLogger;
import org.nuxeo.ecm.platform.importer.log.ImporterLogger;
import org.nuxeo.ecm.platform.importer.threading.DefaultMultiThreadingPolicy;
import org.nuxeo.ecm.platform.importer.threading.ImporterThreadingPolicy;

/**
 *
 * base class for importers
 *
 * @author Thierry Delprat
 *
 */
public abstract class AbstractImporterExecutor {

    protected abstract Log getJavaLogger();

    protected static ImporterLogger log;

    protected static Thread executorMainThread;

    protected static ImporterRunner lastRunner;

    protected ImporterThreadingPolicy threadPolicy;

    protected ImporterDocumentModelFactory factory;

    protected int transactionTimeout = 0;

    public ImporterLogger getLogger() {
        if (log == null) {
            log = new BasicLogger(getJavaLogger());
        }
        return log;
    }

    public String getStatus() {
        if (isRunning()) {
            return "Running";
        } else {
            return "Not Running";
        }
    }

    public boolean isRunning() {
        if (executorMainThread == null) {
            return false;
        } else {
            return executorMainThread.isAlive();
        }
    }

    public String kill() {
        if (executorMainThread != null) {
            if (lastRunner!=null) {
                lastRunner.stopImportProcrocess();
            }
            executorMainThread.interrupt();
            return "Importer killed";
        }
        return "Importer is not running";
    }

    protected void startTask(ImporterRunner runner, boolean interactive) {
        executorMainThread = new Thread(runner);
        executorMainThread.setName("ImporterExecutorMainThread");
        if (interactive) {
            executorMainThread.run();
        } else {
            executorMainThread.start();
        }
    }

    protected String doRun(ImporterRunner runner, Boolean interactive)
            throws Exception {
        if (isRunning()) {
            throw new Exception("Task is already running");
        }
        if (interactive == null) {
            interactive = false;
        }
        lastRunner = runner;
        startTask(runner, interactive);

        if (interactive) {
            return "Task compeleted";
        } else {
            return "Started";
        }
    }

    public ImporterThreadingPolicy getThreadPolicy() {
        if (threadPolicy == null) {
            threadPolicy = new DefaultMultiThreadingPolicy();
        }
        return threadPolicy;
    }

    public void setThreadPolicy(ImporterThreadingPolicy threadPolicy) {
        this.threadPolicy = threadPolicy;
    }

    public ImporterDocumentModelFactory getFactory() {
        if (factory == null) {
            factory = new DefaultDocumentModelFactory();
        }
        return factory;
    }

    public void setFactory(ImporterDocumentModelFactory factory) {
        this.factory = factory;
    }

    /**
     *
     * @since 5.9.4
     *
     */
    public int getTransactionTimeout() {
        return transactionTimeout;
    }

    /**
     *
     * @since 5.9.4
     *
     */
    public void setTransactionTimeout(int transactionTimeout) {
        this.transactionTimeout = transactionTimeout;
    }

    /***
     * since 5.5 this method is invoked when using the
     * <code>DefaultImporterService</code> and passing the executor to the
     * importDocuments method
     *
     * @param runner
     * @param interactive
     * @return
     * @throws Exception
     */
    public String run(ImporterRunner runner, Boolean interactive)
            throws Exception {
        return doRun(runner, interactive);
    }
}
