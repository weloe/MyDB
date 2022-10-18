package com.weloe.mydb.data.pagecache;

import com.weloe.mydb.cache.AbstractCache;
import com.weloe.mydb.data.page.Page;
import com.weloe.mydb.data.page.PageImpl;
import com.weloe.mydb.exception.Error;
import com.weloe.mydb.util.ExceptionHandler;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache{
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;
    private AtomicInteger pageNumbers;

    public PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            ExceptionHandler.systemStop(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buffer);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }finally {
            fileLock.unlock();
        }

        return new PageImpl(pgno,buffer.array(),this);
    }

    @Override
    protected void releaseForCache(Page page) {
        if (page.isDirty()) {
            //如果是脏页面
            flush(page);
            page.setDirty(false);
        }
    }

    /**
     * 写入页数据
     * @param page
     */
    private void flush(Page page) {
        int pageNumber = page.getPageNumber();
        long offset = pageOffset(pageNumber);

        fileLock.lock();
        try {
            ByteBuffer buffer = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buffer);
            fc.force(false);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }finally {
            fileLock.unlock();
        }


    }

    /**
     * 计算偏移量
     * @param pgno
     * @return
     */
    private static long pageOffset(int pgno) {
        //页号从1开始
        return (pgno-1) * PAGE_SIZE;
    }


    @Override
    public int newPage(byte[] initData) throws Exception {
        int pageNo = pageNumbers.incrementAndGet();
        PageImpl page = new PageImpl(pageNo, initData, null);
        flush(page);
        return pageNo;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long) pgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }

    }

    @Override
    public void release(Page page) {
        release((long)page.getPageNumber());
    }

    @Override
    public void truncateByPgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }
        pageNumbers.set(maxPgno);

    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }
}
