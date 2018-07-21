/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage.bean;

public class AppendResult {

    private int position;
    private boolean isFirstRecord;

    public AppendResult(){
        this.isFirstRecord = false;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public boolean isFirstRecord() {
        return isFirstRecord;
    }

    public void setFirstRecord(boolean firstRecord) {
        isFirstRecord = firstRecord;
    }
}
