/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging;

import io.openmessaging.storage.GetMessageService;
import io.openmessaging.storage.PutMessageService;
import io.openmessaging.storage.QueueManager;
import io.openmessaging.storage.bean.QueueEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DefaultQueueStoreImpl extends QueueStore {

    private static final List<byte[]> EMPTY = new ArrayList<>();

    private static ThreadLocal<String> currentQueueName = new ThreadLocal<>();
    private static ThreadLocal<QueueEntry> currentQueue = new ThreadLocal<>();

    private QueueManager queueManager;
    private PutMessageService putMessageService;
    private GetMessageService getMessageService;

    public DefaultQueueStoreImpl(){
        queueManager = QueueManager.getQueueManager();
        putMessageService = PutMessageService.getPutMessageService();
        getMessageService = GetMessageService.getMessageService();
    }

    public void put(String queueName, byte[] message) {
        QueueEntry queueEntry = getQueueEntry(queueName);
        if(queueEntry!=null){
            long nextSeq = queueEntry.nextSeq();
            putMessageService.putMessage(queueEntry.getQueueId(),nextSeq,message);
        }
    }

    public Collection<byte[]> get(String queueName, long offset, long num) {
        QueueEntry queueEntry = getQueueEntry(queueName);
        if(queueEntry!=null){
            return getMessageService.getMessage(queueEntry.getQueueId(),offset,num);
        }else {
            return EMPTY;
        }
    }

    public QueueEntry getQueueEntry(String queueName){
        QueueEntry queueEntry = null;
        if(currentQueueName.get()!=null){
            if(currentQueueName.get().equals(queueName)){
                queueEntry = currentQueue.get();
            }
        }
        if(queueEntry==null){
            currentQueueName.set(queueName);
            queueEntry = queueManager.getQueueEntry(queueName);
            currentQueue.set(queueEntry);
        }
        return queueEntry;
    }

}
