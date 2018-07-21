/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage.bean;

public class IndexRecord {

    private long phyoffset;
    private long beginSeq;

    public IndexRecord(long beginSeq, long phyoffset) {
        this.phyoffset = phyoffset;
        this.beginSeq = beginSeq;
    }

    public long getPhyoffset() {
        return phyoffset;
    }

    public void setPhyoffset(long phyoffset) {
        this.phyoffset = phyoffset;
    }

    public long getBeginSeq() {
        return beginSeq;
    }

    public void setBeginSeq(long beginSeq) {
        this.beginSeq = beginSeq;
    }
}
