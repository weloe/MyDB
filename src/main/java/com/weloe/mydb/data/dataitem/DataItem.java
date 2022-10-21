package com.weloe.mydb.data.dataitem;

import com.google.common.primitives.Bytes;
import com.weloe.mydb.common.SubArray;
import com.weloe.mydb.data.DataManagerImpl;
import com.weloe.mydb.data.page.Page;
import com.weloe.mydb.util.ByteParser;
import com.weloe.mydb.util.Types;

import java.util.Arrays;

public interface DataItem {


    SubArray data();

    /**
     * 修改DataItem前调用
     */
    void before();

    /**
     * 要撤销修改时调用
     */
    void unBefore();

    /**
     * 修改完成后调用
     * @param tranId
     */
    void after(long tranId);

    void release();

    void lock();

    void unlock();

    void rUnLock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();



}
