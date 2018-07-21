/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage.index;

import io.openmessaging.storage.bean.CreateFileRequest;
import io.openmessaging.storage.utils.LogUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class IndexService {

    private IndexFile lastFile;
    private Map<Integer,IndexFilePair> indexFiles;
    private CopyOnWriteArrayList<IndexFile> indexFileList;

    private static IndexService indexService;
    private static Object lock = new Object();

    private IndexService(){
        indexFiles = new ConcurrentHashMap<>();
        indexFileList = new CopyOnWriteArrayList<>();
        lastFile = createFile(new CreateFileRequest(-1));
    }

    public static IndexService getIndexService(){
        if(indexService==null){
            synchronized (lock){
                if(indexService==null){
                    indexService = new IndexService();
                }
            }
        }
        return indexService;
    }

    private synchronized IndexFile createFile(CreateFileRequest request){
        int fileId = request.getLastFileId() + 1 ;
        if(!indexFiles.containsKey(fileId)){
            try {
                IndexFile indexFile = new IndexFile(fileId);
                indexFiles.put(fileId,new IndexFilePair(request,indexFile));
                indexFileList.add(indexFile);
            }catch (Exception ex){
                LogUtil.error("Create File Failed",ex);
            }
        }
        return indexFiles.get(fileId).getIndexFile();
    }

    public IndexFile getLastFile(){
        return this.lastFile;
    }

    public IndexFile needNewFile(int lastFileId){
        synchronized (lastFile){
            this.lastFile = createFile(new CreateFileRequest(lastFileId));
        }
        return getLastFile();
    }


    public List<IndexFile> getAllIndexFiles(){
        return this.indexFileList;
    }

    private static class IndexFilePair{

        private CreateFileRequest request;
        private IndexFile indexFile;

        public IndexFilePair(CreateFileRequest request, IndexFile indexFile) {
            this.request = request;
            this.indexFile = indexFile;
        }

        public CreateFileRequest getRequest() {
            return request;
        }

        public IndexFile getIndexFile() {
            return indexFile;
        }
    }

}
