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

import org.nuxeo.ecm.core.api.Blob;

import java.io.IOException;


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
