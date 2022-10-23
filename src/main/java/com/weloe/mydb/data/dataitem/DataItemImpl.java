package com.weloe.mydb.data.dataitem;

import com.weloe.mydb.common.SubArray;
import com.weloe.mydb.data.DataManagerImpl;
import com.weloe.mydb.data.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 *                  1 : 0/1         2         DataSize
 * SubArray.raw:  [ValidFlag] [DataSize] [Data]
 */
public class DataItemImpl implements DataItem {

    public static final int OF_VALID = 0;
    public static final int OF_SIZE = 1;
    public static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dm;
    private long uid;
    private Page pg;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    /**
     * 判断DataItem是否有效
     *
     * @return
     */
    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte) 0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        // 拷贝保留源数据到oldRaw
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        // 把源数据拷贝回raw
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
    }

    @Override
    public void after(long tranId) {
        dm.logDataItem(tranId, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
