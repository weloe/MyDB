package com.weloe.mydb.data;

import com.weloe.mydb.cache.AbstractCache;
import com.weloe.mydb.data.dataitem.DataItem;
import com.weloe.mydb.data.dataitem.DataItemImpl;
import com.weloe.mydb.data.logger.Logger;
import com.weloe.mydb.data.page.Page;
import com.weloe.mydb.data.page.PageCommon;
import com.weloe.mydb.data.page.PageOne;
import com.weloe.mydb.data.pagecache.PageCache;
import com.weloe.mydb.data.pageindex.PageIndex;
import com.weloe.mydb.data.pageindex.PageInfo;
import com.weloe.mydb.exception.Error;
import com.weloe.mydb.transaction.TransactionManager;
import com.weloe.mydb.util.ExceptionHandler;
import com.weloe.mydb.util.Types;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItem = (DataItemImpl) super.get(uid);
        if(!dataItem.isValid()){
            //无效数据
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    /**
     * 在 pageIndex 中获取一个足以存储插入内容的页面的页号
     * 获取页面后，写入插入日志
     * 通过 pageCommon 插入数据，并返回插入位置的偏移。
     * 最后需要将页面信息重新插入 pageIndex。
     * @param tranId
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long tranId, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageCommon.MAX_FREE_SPACE){
            throw Error.DataTooLargeException;
        }
        PageInfo pageInfo = null;
        //在 pageIndex 中获取一个足以存储插入内容的页面的页号
        for (int i = 0; i < 5; i++) {
            pageInfo = pIndex.select(raw.length);
            if(pageInfo != null){
                break;
            }else {
                int newPageNo = pc.newPage(PageCommon.initRaw());
                pIndex.add(newPageNo,PageCommon.MAX_FREE_SPACE);
            }
        }
        if(pageInfo == null){
            throw Error.DatabaseBusyException;
        }

        Page page = null;
        int freeSpace = 0;
        try {
            //记录日志
            byte[] log = Recover.insertLog(tranId, page, raw);
            logger.log(log);
            //插入数据
            short offset = PageCommon.insert(page, raw);
            page.release();
            return Types.addressToUid(pageInfo.pageNo,offset);
        } finally {
            //将取出的page重新插入pIndex
            if(page != null){
                pIndex.add(pageInfo.pageNo, PageCommon.getFreeSpace(page));
            }else {
                pIndex.add(pageInfo.pageNo,freeSpace);
            }
        }

    }

    @Override
    public void close() {
        super.close();
        logger.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }


    /**
     * 获取所有page并填充pageIndex
     */
    void fillPageIndex(){
        int pageNumber = pc.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) {
            Page page = null;
            try {
                page = pc.getPage(i);
            }catch (Exception e){
                ExceptionHandler.systemStop(e);
            }
            pIndex.add(page.getPageNumber(), PageCommon.getFreeSpace(page));
            //release
            page.release();
        }

    }

    /**
     * 为tranId生成update日志
     * @param tranId
     * @param dataItem
     */
    public void logDataItem(long tranId, DataItemImpl dataItem) {
        byte[] log = Recover.updateLog(tranId, dataItem);
        logger.log(log);
    }

    public void releaseDataItem(DataItemImpl dataItem) {
        super.release(dataItem.getUid());
    }

    /**
     * uid -- 页号+页内偏移组成的8字节无符号整数，各占4字节
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    /**
     * 创建文件时初始化第一个数据页
     */
    public void initPageOne() {
        int pageNo = pc.newPage(PageOne.initRaw());
        assert pageNo == 1;
        try {
            pageOne = pc.getPage(pageNo);
        }catch (Exception e){
            ExceptionHandler.systemStop(e);
        }
        pc.flushPage(pageOne);
    }

    /**
     * 在打开已有的文件时读入PageOne，并校验
     * @return
     */
    public boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            ExceptionHandler.systemStop(e);
        }
        return PageOne.checkVc(pageOne);
    }
}
