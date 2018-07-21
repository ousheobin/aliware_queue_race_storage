/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage.index;

import io.openmessaging.storage.bean.IndexRecord;
import io.openmessaging.storage.config.Constants;
import io.openmessaging.storage.utils.LRUMap;
import io.openmessaging.storage.utils.LogUtil;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IndexFile {

    private AtomicInteger curPos;
    private AtomicInteger size;
    private int indexId;
    private ReadWriteLock readWriteLock;

    private RandomAccessFile file;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    public IndexFile(int indexId) throws Exception{
        this.indexId = indexId;
        this.curPos = new AtomicInteger(0);
        this.size = new AtomicInteger(0);
        this.readWriteLock = new ReentrantReadWriteLock();

        this.file = new RandomAccessFile(Constants.DATA_PATH+"/"+indexId+".index","rw");
        this.fileChannel = this.file.getChannel();
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE,0,Constants.SIZE_PRE_COMMON_FILE);
        this.initHeader();

        LogUtil.info("Create Index File Successfully:" + Constants.DATA_PATH+"/"+indexId+".index");

    }

    public void initHeader(){
        this.mappedByteBuffer.slice().putInt(Constants.VALID_HEADER);
        this.size.getAndAdd(Constants.HEADER_SIZE + 4 * Constants.SLOT_PRE_INDEX_FILE);
        this.curPos.getAndAdd(Constants.HEADER_SIZE + 4 * Constants.SLOT_PRE_INDEX_FILE);
    }

    public int writeIndex(int queueId,long beginSeq,long phyOffset){
        this.readWriteLock.writeLock().lock();
        int position = -1;
        boolean writeable = (this.size.addAndGet(Constants.DEFAULT_INDEX_LENGTH) <= Constants.SIZE_PRE_COMMON_FILE);
        if(writeable){
            position = this.curPos.getAndAdd(Constants.DEFAULT_INDEX_LENGTH);
            int slotPosition = getSlotPosition(queueId);
            try{
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                int lastPosition = byteBuffer.getInt(slotPosition);
                byteBuffer.putInt(slotPosition,position);
                byteBuffer.position(position);
                byteBuffer.putInt(queueId);
                byteBuffer.putLong(beginSeq);
                byteBuffer.putInt(lastPosition);
                byteBuffer.putLong(phyOffset);
            }catch (Exception ex){
                position = -1;
            }
        }
        this.readWriteLock.writeLock().unlock();
        return position;
    }

    public List<Long> getIndexList(int queueId,long beginSeq,long endSeq){
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int slotPos = getSlotPosition(queueId);
        int curPos = byteBuffer.getInt(slotPos);
        LinkedList<Long> result = new LinkedList<>();

        while ( curPos > 0 ){
            byteBuffer.position(curPos);
            int curQueue = byteBuffer.getInt();
            long curBeginSeq = byteBuffer.getLong();
            curPos = byteBuffer.getInt();
            long curEndSeq = curBeginSeq + Constants.INDEX_MARK_INTERVAL;
            if( curQueue == queueId ){
                if( ( curBeginSeq <= beginSeq && beginSeq <= curEndSeq ) || ( curBeginSeq <= endSeq && endSeq <= curEndSeq ) ){
                    result.addFirst(byteBuffer.getLong());
                }else if( curEndSeq < beginSeq){
                    break;
                }
            }
        }

        return result;
    }

    public int getIndexId() {
        return indexId;
    }

    public int getSlotPosition(int queueId){
        return (queueId%Constants.SLOT_PRE_INDEX_FILE) * 4 + Constants.HEADER_SIZE;
    }

}

