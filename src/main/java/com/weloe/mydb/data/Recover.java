package com.weloe.mydb.data;

import com.google.common.primitives.Bytes;
import com.weloe.mydb.common.SubArray;
import com.weloe.mydb.data.dataitem.DataItem;
import com.weloe.mydb.data.logger.Logger;
import com.weloe.mydb.data.page.Page;
import com.weloe.mydb.data.page.PageCommon;
import com.weloe.mydb.data.pagecache.PageCache;
import com.weloe.mydb.transaction.TransactionManager;
import com.weloe.mydb.util.ByteParser;
import com.weloe.mydb.util.ExceptionHandler;

import java.util.*;


public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    /**
     * updateLog: [LogType] [TranId] [UID] [OldRaw] [NewRaw]
     * insertLog: [LogType] [TranId] [Offset] [Raw]
     */
    static class InsertLogInfo {
        long tranId;
        int pageNo;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long tranId;
        int pageNo;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger logger, PageCache pageCache)   {
        System.out.println("Recovering...");

        logger.rewind();
        int maxPageNo = 0;
        while (true) {
            byte[] log = logger.next();
            if (log == null) {
                break;
            }
            int pageNo;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pageNo = li.pageNo;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pageNo = li.pageNo;
            }

            if (pageNo > maxPageNo) {
                maxPageNo = pageNo;
            }
        }
        pageCache.truncateByPgno(maxPageNo);
        System.out.println("Truncate to " + maxPageNo + "pages.");

        redoTransaction(tm, logger, pageCache);
        System.out.println("Redo Transaction Over");

        undoTransactions(tm, logger, pageCache);
        System.out.println("Undo Transaction Over");

        System.out.println("Recovery Over");

    }


    /**
     * 恢复数据
     *
     * @param tm
     * @param lg
     * @param pageCache
     */
    private static void redoTransaction(TransactionManager tm, Logger lg, PageCache pageCache) {
        //重置日志指针
        lg.rewind();

        while (true) {
            byte[] log = lg.next();
            if (log == null) {
                break;
            }
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long tranId = li.tranId;
                if (!tm.isActive(tranId)) {
                    doInsertLog(pageCache, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long tranId = xi.tranId;
                if (!tm.isActive(tranId)) {
                    doUpdateLog(pageCache, log, REDO);
                }
            }

        }
    }


    private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pageCache)  {
        HashMap<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) {
                break;
            }
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long tranId = li.tranId;
                if (tm.isActive(tranId)) {
                    if (!logCache.containsKey(tranId)) {
                        logCache.put(tranId, new ArrayList<>());
                    }
                    logCache.get(tranId).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long tranId = xi.tranId;
                if (tm.isActive(tranId)) {
                    if (!logCache.containsKey(tranId)) {
                        logCache.put(tranId, new ArrayList<>());
                    }
                    logCache.get(tranId).add(log);
                }
            }

        }

        //遍历所有的active log 进行倒序
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i++) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pageCache, log, UNDO);
                } else {
                    doUpdateLog(pageCache, log, UNDO);
                }
            }
            //把事务更改为撤销状态
            tm.abort(entry.getKey());
        }

    }


    // updateLog: [LogType] [TranId] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_TRAN_ID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_TRAN_ID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    /**
     * updateLog
     * [LogType] [TranId] [UID] [OldRaw] [NewRaw]
     *
     * @param tranId
     * @param dataItem
     * @return
     */
    public static byte[] updateLog(long tranId, DataItem dataItem) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] tranIdRaw = ByteParser.long2Byte(tranId);
        byte[] uidRaw = ByteParser.long2Byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();
        SubArray subArrayRaw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(subArrayRaw.raw, subArrayRaw.start, subArrayRaw.end);
        return Bytes.concat(logType, tranIdRaw, uidRaw, oldRaw, newRaw);
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.tranId = ByteParser.parseLong(Arrays.copyOfRange(log, OF_TRAN_ID, OF_UPDATE_UID));
        long uid = ByteParser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        //todo
        li.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pageNo = (int) (uid & ((1L << 32) - 1));
        int length =  (log.length - OF_UPDATE_RAW) /2;
        li.oldRaw = Arrays.copyOfRange(log,OF_UPDATE_RAW,OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log,OF_UPDATE_RAW+length,OF_UPDATE_RAW+length * 2);
        return li;
    }

    private static void doUpdateLog(PageCache pageCache, byte[] log, int flag) {
        int pageNo;
        short offset;
        byte[] raw;

        if (flag == REDO) {
            //如果是REDO
            UpdateLogInfo xi = parseUpdateLog(log);
            pageNo = xi.pageNo;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            //UNDO
            UpdateLogInfo xi = parseUpdateLog(log);
            pageNo = xi.pageNo;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page page = null;
        try {
            page = pageCache.getPage(pageNo);
        } catch (Exception e) {
            ExceptionHandler.systemStop(e);
        }

        try {
            assert page != null;
            PageCommon.recoverUpdate(page, raw, offset);
        } finally {
            //释放缓存的数据页
            page.release();
        }

    }

    private static boolean isUpdateLog(byte[] log) {
        return log[0] == LOG_TYPE_UPDATE;
    }

    // [LogType] [TranId] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_TRAN_ID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    public static byte[] insertLog(long tranId,Page pg,byte[] raw){
        byte[] logType = {LOG_TYPE_INSERT};
        byte[] tranIdRaw = ByteParser.long2Byte(tranId);
        byte[] pgnoRaw = ByteParser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = ByteParser.short2Byte(PageCommon.getFSO(pg));
        return Bytes.concat(logType,tranIdRaw,pgnoRaw,offsetRaw);
    }

    //todo
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.tranId = ByteParser.parseLong(Arrays.copyOfRange(log,OF_TRAN_ID,OF_INSERT_PGNO));
        li.pageNo = ByteParser.parseShort(Arrays.copyOfRange(log,OF_INSERT_OFFSET,OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log,OF_INSERT_RAW,log.length);
        return li;
    }

    private static void doInsertLog(PageCache pageCache, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page page = null;
        try {
            page = pageCache.getPage(li.pageNo);
        } catch (Exception e) {
            ExceptionHandler.systemStop(e);
        }
        try {
            if (flag == UNDO) {
                // UNDO
                DataItem.setDataItemRawInvalid(li.raw);
            }
            assert page != null;
            PageCommon.recoverInsert(page, li.raw, li.offset);
        } finally {
            //释放缓存的数据页
            page.release();
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }


}
