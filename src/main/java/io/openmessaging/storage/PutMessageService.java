/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage;

import io.openmessaging.storage.bean.AppendResult;
import io.openmessaging.storage.config.Constants;
import io.openmessaging.storage.data.DataFile;
import io.openmessaging.storage.data.DataService;
import io.openmessaging.storage.index.IndexFile;
import io.openmessaging.storage.index.IndexService;
import io.openmessaging.storage.utils.LogUtil;

public class PutMessageService {

    private DataService dataService;
    private IndexService indexService;

    private static PutMessageService putMessageService;
    private static Object lock = new Object();

    private PutMessageService(){
        dataService = DataService.getDataService();
        indexService = IndexService.getIndexService();
    }

    public static PutMessageService getPutMessageService(){
        if(putMessageService==null){
            synchronized (lock){
                if(putMessageService==null){
                    putMessageService = new PutMessageService();
                }
            }
        }
        return putMessageService;
    }

    public void putMessage(int queueId,long seq,byte[] content){

        DataFile dataFile = dataService.getLastFile();

        if(dataFile==null){
            dataFile = dataService.needNewFile(-1);
        }

        AppendResult appendResult = dataFile.addData(queueId,seq,content);

        int lastFileId = dataFile.getFileId();

        if(appendResult.getPosition() < 0 ){
            dataFile = dataService.needNewFile(dataFile.getFileId());
            appendResult = dataFile.addData(queueId,seq,content);
        }

        if(appendResult.getPosition() < 0 ){
            LogUtil.error("Write Data Error, FileId:"+dataFile.getFileId()+", Last File Id:"+lastFileId);
            return;
        }

        if( appendResult.isFirstRecord() || seq % Constants.INDEX_MARK_INTERVAL == 0 ){
            writeIndex(queueId,seq,
                    (long) Constants.SIZE_PRE_COMMON_FILE * dataFile.getFileId() + appendResult.getPosition());
        }

    }

    private void writeIndex(int queueId,long seqId,long phyOffset){
        IndexFile indexFile = indexService.getLastFile();

        if(indexFile == null){
            indexFile = indexService.needNewFile(-1);
        }

        int writePosition = indexFile.writeIndex(queueId,seqId,phyOffset);

        if(writePosition < 0 ){
            indexFile = indexService.needNewFile(indexFile.getIndexId());
            writePosition = indexFile.writeIndex(queueId,seqId,phyOffset);
        }

        if(writePosition < 0 ){
            LogUtil.error("Write Index Error, FileId:"+indexFile.getIndexId());
            return;
        }
    }


}
