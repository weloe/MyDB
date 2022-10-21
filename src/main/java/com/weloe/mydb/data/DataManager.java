package com.weloe.mydb.data;

import com.weloe.mydb.data.dataitem.DataItem;
import com.weloe.mydb.data.logger.Logger;
import com.weloe.mydb.data.page.PageOne;
import com.weloe.mydb.data.pagecache.PageCache;
import com.weloe.mydb.transaction.TransactionManager;

public interface DataManager {
    /**
     * 读数据
     * @param uid
     * @return
     * @throws Exception
     */
    DataItem read(long uid) throws Exception;

    /**
     * 插入数据
     * @param tranId
     * @param data
     * @return
     * @throws Exception
     */
    long insert(long tranId,byte[] data)throws Exception;

    void close();

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        //初始化第一页
        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long mem, TransactionManager tm) throws Exception {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        //校验第一页
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
