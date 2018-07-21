/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage;

import io.openmessaging.storage.config.Constants;
import io.openmessaging.storage.data.DataService;
import io.openmessaging.storage.index.IndexFile;
import io.openmessaging.storage.index.IndexService;
import io.openmessaging.storage.utils.LogUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class GetMessageService {

    private static GetMessageService getMessageService;
    private static Object lock = new Object();

    private DataService dataService;
    private IndexService indexService;

    private GetMessageService() {
        dataService = DataService.getDataService();
        indexService = IndexService.getIndexService();
    }

    public static GetMessageService getMessageService() {
        if (getMessageService == null) {
            synchronized (lock) {
                if (getMessageService == null) {
                    getMessageService = new GetMessageService();
                }
            }
        }
        return getMessageService;
    }

    public List<byte[]> getMessage(int queueId, long offset, long num) {
        long end = offset + num;
        List<Long> phyOffsets = getPhyOffsets(queueId, offset, end);
        List<byte[]> result = new LinkedList<>();
        int lastFile = -1;
        for (long phyOffset : phyOffsets) {
            int file = (int) ( phyOffset / Constants.SIZE_PRE_COMMON_FILE ) ;
            if (lastFile != file) {
                lastFile = file;
                result.addAll(dataService.getData(phyOffset, queueId, offset, end));
            }
        }

        return result;
    }

    public List<Long> getPhyOffsets(int queueId, long offset, long end) {
        List<IndexFile> indexFiles = indexService.getAllIndexFiles();
        List<Long> result = new ArrayList<>();
        if (indexFiles != null && !indexFiles.isEmpty()) {
            for (int i = 0; i < indexFiles.size(); i++) {
                IndexFile indexFile = indexFiles.get(i);
                if (indexFile != null) {
                    try {
                        result.addAll(indexFile.getIndexList(queueId, offset, end));
                    } catch (Exception ex) {
                        LogUtil.error("GetIndex Error", ex);
                    }
                }
            }
        }
        return result;
    }
}
