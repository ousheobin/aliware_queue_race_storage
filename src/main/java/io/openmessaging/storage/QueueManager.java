/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage;

import io.openmessaging.storage.bean.QueueEntry;
import io.openmessaging.storage.utils.LogUtil;
import io.openmessaging.storage.utils.TrieTree;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class QueueManager {

    private static final AtomicInteger QUEUE_ID = new AtomicInteger(0);
    private static Object lock = new Object();
    private static QueueManager queueManager;

    private TrieTree<QueueEntry> queueMap;
    private ReentrantLock reentrantLock;

    private QueueManager(){
        this.queueMap = new TrieTree<>();
        this.reentrantLock = new ReentrantLock();
    }

    public static QueueManager getQueueManager(){
        if(queueManager==null){
            synchronized (lock){
                if(queueManager==null){
                    queueManager = new QueueManager();
                    LogUtil.info("Init Queue Manager Ok!");
                }
            }
        }
        return queueManager;
    }

    private QueueEntry putQueue(String queueName){
        this.reentrantLock.lock();
        QueueEntry queueEntry = this.queueMap.get(queueName);
        if(queueEntry == null){
            queueEntry = new QueueEntry(QUEUE_ID.getAndIncrement());
            queueMap.put(queueName,queueEntry);
        }
        this.reentrantLock.unlock();
        return queueEntry;
    }

    public QueueEntry getQueueEntry(String queueName){
        QueueEntry queueEntry = this.queueMap.get(queueName);
        if(queueEntry==null){
            queueEntry = putQueue(queueName);
        }
        return queueEntry;
    }

}
