package org.nuxeo.ecm.platform.importer.kafka.message;

import org.nuxeo.ecm.core.api.Blob;

import java.io.IOException;

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
