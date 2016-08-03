package org.nuxeo.ecm.platform.importer.kafka.message;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.nuxeo.ecm.core.api.blobholder.BlobHolder;
import org.nuxeo.ecm.platform.importer.source.SourceNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by anechaev on 8/3/16.
 * © Andrei Nechaev 2016
 */
public class Message {

    private String mName;
    private String mPath;
    private boolean isFolderish;
    private transient BlobHolder mBlobHolder;
    private Map<String, Serializable> mProperties;
    private List<Data> mData;

    public Message() {
    }

    public Message(SourceNode node) throws IOException {
        this.mName = node.getName();
        this.mPath = node.getSourcePath();

        this.isFolderish = node.isFolderish();
        this.mBlobHolder = node.getBlobHolder();

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

    @JsonIgnore
    public BlobHolder getBlobHolder() {
        return mBlobHolder;
    }

    @JsonProperty
    public void setBlobHolder(BlobHolder mBlobHolder) {
        this.mBlobHolder = mBlobHolder;
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

    @Override
    public String toString() {
        return "Message{" +
                "mName='" + mName + '\'' +
                ", mPath='" + mPath + '\'' +
                ", isFolderish=" + isFolderish +
                ", mBlobHolder=" + mBlobHolder +
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
        return mPath.equals(message.mPath);

    }

    @Override
    public int hashCode() {
        int result = mName.hashCode();
        result = 31 * result + mPath.hashCode();
        result = 31 * result + (isFolderish ? 1 : 0);
        return result;
    }
}
