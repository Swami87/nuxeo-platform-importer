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

package org.nuxeo.ecm.platform.importer.kafka.message;

import org.nuxeo.ecm.platform.importer.source.SourceNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;


public class Message {

    private String mName;
    private String mPath;
    private boolean isFolderish;
    private Map<String, Serializable> mProperties;
    private List<Data> mData;
    private String mDigest;
    private String mHash;
    private String mParentHash;

    public Message() {
        mHash = UUID.randomUUID().toString();
    }

    public Message(SourceNode node) throws IOException {
        this.mName = node.getName();
        this.mPath = node.getSourcePath();

        this.isFolderish = node.isFolderish();

        if (node.getBlobHolder() != null) {
            this.mProperties = node.getBlobHolder().getProperties();
            mData = DataFactory.createData(node.getBlobHolder());
        }
    }


    public String getName() {
        return mName;
    }

    public void setName(String mName) {
        this.mName = mName;
    }


    public String getPath() {
        return mPath;
    }

    public void setPath(String mPath) {
        this.mPath = mPath;
    }


    public boolean isFolderish() {
        return isFolderish;
    }

    public void setFolderish(boolean folderish) {
        isFolderish = folderish;
    }


    public List<Data> getData() {
        return mData;
    }

    public void setData(List<Data> data) {
        this.mData = data;
    }


    public Map<String, Serializable> getProperties() {
        return mProperties;
    }

    public void setProperties(Map<String, Serializable> properties) {
        this.mProperties = properties;
    }


    public void setHash(String hash) {
        this.mHash = hash;
    }

    public String getHash() {
        return mHash;
    }

    public String getParentHash() {
        return mParentHash;
    }

    public void setParentHash(String parentHash) {
        this.mParentHash = parentHash;
    }


    public String getDigest() {
        return mDigest;
    }

    public void setDigest(String digest) {
        this.mDigest = digest;
    }


    @Override
    public String toString() {
        return "Message{" +
                "mName='" + mName + '\'' +
                ", mPath='" + mPath + '\'' +
                ", isFolderish=" + isFolderish +
                ", mProperties=" + mProperties +
                ", mData=" + mData +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message)) return false;

        Message message = (Message) o;

        if (isFolderish != message.isFolderish) return false;
        if (!mName.equals(message.mName)) return false;
        return mPath != null && mPath.equals(message.mPath);
    }

    @Override
    public int hashCode() {
        int result = mName.hashCode();
        result = 31 * result + (mPath != null ? mPath.hashCode() : 0);
        result = 31 * result + (isFolderish ? 1 : 0);
        return result;
    }
}
