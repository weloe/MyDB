package com.weloe.mydb.data.page;

import com.weloe.mydb.data.pagecache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page {
    // 页面的页号
    private int pageNumber;
    // 页面包含的字节数据
    private byte[] data;
    // 是否是脏页面
    private boolean dirty;
    // lock
    private Lock lock;

    private PageCache pc;

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
