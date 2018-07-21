/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage.data;

import io.openmessaging.storage.bean.CreateFileRequest;
import io.openmessaging.storage.config.Constants;
import io.openmessaging.storage.utils.LogUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataService {

    private volatile DataFile lastFile;
    private Map<Integer,DataFilePair> dataFiles;

    private static DataService dataService;
    private static Object lock = new Object();

    private static final List<byte[]> EMPTY = new ArrayList<>();

    private DataService(){
        dataFiles = new ConcurrentHashMap<>();
        lastFile = createFile(new CreateFileRequest(-1));
        LogUtil.info("Data Service Init!");
    }

    public static DataService getDataService(){
        if(dataService==null){
            synchronized (lock){
                if(dataService==null){
                    dataService = new DataService();
                }
            }
        }
        return dataService;
    }

    private synchronized DataFile createFile(CreateFileRequest request){
        int fileId = request.getLastFileId() + 1 ;
        if(!dataFiles.containsKey(fileId)){
            try {
                dataFiles.put(fileId,new DataFilePair(request,new DataFile(fileId)));
            }catch (Exception ex){
                LogUtil.error("Create File Failed",ex);
            }
        }
        return dataFiles.get(fileId).getDataFile();
    }

    public DataFile getLastFile(){
        return this.lastFile;
    }

    public DataFile needNewFile(int lastFileId){
        synchronized (lastFile){
            this.lastFile = createFile(new CreateFileRequest(lastFileId));
        }
        return getLastFile();
    }

    public List<byte[]> getData(long beginOffset,int queueId, long beginId,long endId) {
        DataFilePair dataFilePair = this.dataFiles.get((int)( beginOffset / Constants.SIZE_PRE_COMMON_FILE ));
        try{
            if(dataFilePair!=null && dataFilePair.getDataFile() != null){
                return dataFilePair.getDataFile().getData((int)( beginOffset % Constants.SIZE_PRE_COMMON_FILE )
                        ,queueId,beginId,endId);
            }
        }catch (IllegalArgumentException ex){

        }
        LogUtil.error("Get Data Error  beginOffset:"+beginOffset+" Queue Id:"+queueId+" File:"+(int) beginOffset / Constants.SIZE_PRE_COMMON_FILE);
        return EMPTY;
    }

    private static class DataFilePair{

        private CreateFileRequest request;
        private DataFile dataFile;

        public DataFilePair(CreateFileRequest request, DataFile dataFile) {
            this.request = request;
            this.dataFile = dataFile;
        }

        public CreateFileRequest getRequest() {
            return request;
        }

        public DataFile getDataFile() {
            return dataFile;
        }
    }

}
