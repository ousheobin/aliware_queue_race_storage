/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage.config;

public class Constants {

    private static final int KB = 1024;
    private static final int MB = 1024 * KB;
    private static final int GB = 1024 * MB;

    public static final String DATA_PATH = System.getProperty("mq.storage.dataPath","/alidata1/race2018/data");

    public static final int SIZE_PRE_COMMON_FILE = 1 * GB;

    public static final int VALID_HEADER = 0x000b5508;
    public static final int HEADER_SIZE = 4;
    public static final int SLOT_PRE_INDEX_FILE = 2000000;
    public static final int FLUSH_PAGE_SIZE = 1 * MB;
    public static final int INDEX_MARK_INTERVAL = 16;

    public static final int MESSAGE_DESC_LENGTH = 4 +  // Queue ID 4 byte 队列ID
            8 + // Sequence ID 8 byte 当前序列
            4 + // Last Position 4 byte 前驱节点位置
            4; // Length 4 byte 数据长度

    public static final int DEFAULT_INDEX_LENGTH = 4 +  // Queue ID 4 byte 队列ID
            8 + // Begin Sequence ID 8 byte 起始序列ID
            8 + // Physical Offset 8 byte 物理位移
            4; // Last Position 4 byte 前驱节点位置

}
