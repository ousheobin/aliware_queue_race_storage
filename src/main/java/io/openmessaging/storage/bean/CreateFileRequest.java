/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage.bean;

public class CreateFileRequest {

    private int lastFileId;
    private long createTime;

    public CreateFileRequest(int lastFileId) {
        this.lastFileId = lastFileId;
    }

    public int getLastFileId() {
        return lastFileId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void finishCreate(){
        this.createTime = System.currentTimeMillis();
    }

}
