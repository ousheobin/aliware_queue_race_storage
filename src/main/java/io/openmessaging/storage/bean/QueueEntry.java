/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage.bean;

import java.util.concurrent.atomic.AtomicLong;

public class QueueEntry{

    private int queueId;
    private AtomicLong seq;

    public QueueEntry(int queueId){
        this.queueId = queueId;
        this.seq = new AtomicLong(0);
    }

    public int getQueueId() {
        return queueId;
    }

    public long nextSeq(){
        return seq.getAndIncrement();
    }

    @Override
    public String toString() {
        return "QueueEntry{" +
                "queueId=" + queueId +
                ", seq=" + seq +
                '}';
    }
}
