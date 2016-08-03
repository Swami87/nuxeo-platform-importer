package org.nuxeo.ecm.platform.importer.kafka.message;

import org.nuxeo.ecm.core.api.Blob;

import java.io.IOException;

/**
 * Created by anechaev on 8/3/16.
 * © Andrei Nechaev 2016
 */
public class Data {


    private byte[] mBytes;
    private String mFileName;
    private String mDigest;
    private String mEncoding;
    private long mLength;
    private String mMimeType;

    public Data() {
    }


    public Data(Blob blob) throws IOException {
        this.mBytes = blob.getByteArray();
        this.mDigest = blob.getDigest();
        this.mEncoding = blob.getEncoding();
        this.mFileName = blob.getFilename();
        this.mLength = blob.getLength();
        this.mMimeType = blob.getMimeType();
    }


    public byte[] getBytes() {
        return mBytes;
    }

    public void setBytes(byte[] bytes) {
        this.mBytes = bytes;
    }

    public String getFileName() {
        return mFileName;
    }

    public void setFileName(String mFileName) {
        this.mFileName = mFileName;
    }

    public String getDigest() {
        return mDigest;
    }

    public void setDigest(String mDigest) {
        this.mDigest = mDigest;
    }

    public String getEncoding() {
        return mEncoding;
    }

    public void setEncoding(String mEncoding) {
        this.mEncoding = mEncoding;
    }

    public long getLength() {
        return mLength;
    }

    public void setLength(long mLength) {
        this.mLength = mLength;
    }

    public String getMimeType() {
        return mMimeType;
    }

    public void setMimeType(String mMimeType) {
        this.mMimeType = mMimeType;
    }

    @Override
    public String toString() {
        return "Data{" +
                ", mFileName='" + mFileName + '\'' +
                ", mDigest='" + mDigest + '\'' +
                ", mEncoding='" + mEncoding + '\'' +
                ", mLength=" + mLength +
                ", mMimeType='" + mMimeType + '\'' +
                '}';
    }
}
