/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage.data;

import io.openmessaging.storage.bean.AppendResult;
import io.openmessaging.storage.config.Constants;
import io.openmessaging.storage.utils.LogUtil;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DataFile {

    private AtomicInteger curPos;
    private AtomicInteger size;
    private int fileId;

    private RandomAccessFile file;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    public DataFile(int fileId) throws Exception{
        this.fileId = fileId;
        this.curPos = new AtomicInteger(0);
        this.size = new AtomicInteger(0);

        this.file = new RandomAccessFile(Constants.DATA_PATH+"/"+fileId+".data","rw");
        this.fileChannel = this.file.getChannel();
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE,0,Constants.SIZE_PRE_COMMON_FILE);
        this.initHeader();

        LogUtil.info("Create Data File Successfully:" + Constants.DATA_PATH+"/"+fileId+".data");
    }

    public void initHeader(){
        this.mappedByteBuffer.slice().putInt(Constants.VALID_HEADER);
        this.size.getAndAdd(Constants.HEADER_SIZE + 4 * Constants.SLOT_PRE_INDEX_FILE);
        this.curPos.getAndAdd(Constants.HEADER_SIZE + 4 * Constants.SLOT_PRE_INDEX_FILE);
    }

    public AppendResult addData(int queueId, long seq, byte[] data){
        AppendResult appendResult = new AppendResult();
        int position = -1;
        int length = Constants.MESSAGE_DESC_LENGTH + data.length;
        boolean writeable = (this.size.addAndGet(length) <= Constants.SIZE_PRE_COMMON_FILE);
        if(writeable){
            position = this.curPos.getAndAdd(length);
            int slotPosition = getSlotPosition(queueId);
            try{
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();

                byteBuffer.position(position);
                byteBuffer.putInt(queueId);
                byteBuffer.putLong(seq);
                byteBuffer.putInt(0);
                byteBuffer.putInt(data.length);
                byteBuffer.put(data);

                int prevPosition = byteBuffer.getInt(slotPosition);
                byteBuffer.putInt(slotPosition,position);

                if( prevPosition != 0 && prevPosition < Constants.SIZE_PRE_COMMON_FILE ){
                    byteBuffer.position(prevPosition + 12 );
                    byteBuffer.putInt(position);
                }else if( prevPosition == 0){
                    appendResult.setFirstRecord(true);
                }
            }catch (Exception ex){
                LogUtil.error("Write Data Exception",ex);
                position = -1;
            }
        }
        appendResult.setPosition(position);
        return appendResult;
    }

    public List<byte[]> getData(int startOffset,int queueId,long startSeq,long endSeq){
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(startOffset);
        List<byte[]> result = new ArrayList<>();
        int curPos = startOffset;
        try{
            while (curPos>0){
                byteBuffer.position(curPos);
                int curQueue = byteBuffer.getInt();
                long curSeq = byteBuffer.getLong();
                curPos = byteBuffer.getInt();
                if(curQueue == queueId){
                    if( curSeq >= startSeq && curSeq < endSeq){
                        int length = byteBuffer.getInt();
                        byte[] content = new byte[length];
                        byteBuffer.get(content);
                        result.add(content);
                    }else if( curSeq >= endSeq ){
                        break;
                    }
                }
            }
        }catch (IllegalArgumentException ex){
            LogUtil.error("Exception ByteBuffer =>"+byteBuffer +" Offset=>"+startOffset);
            throw ex;
        }
        return result;
    }

    public int getSlotPosition(int queueId){
        return (queueId%Constants.SLOT_PRE_INDEX_FILE) * 4 + Constants.HEADER_SIZE;
    }

    public int getFileId() {
        return fileId;
    }

}
