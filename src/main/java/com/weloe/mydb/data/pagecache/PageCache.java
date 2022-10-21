package com.weloe.mydb.data.pagecache;

import com.weloe.mydb.data.page.Page;
import com.weloe.mydb.exception.Error;
import com.weloe.mydb.util.ExceptionHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {
    int PAGE_SIZE = 1 << 13;

    /**
     * 新建数据页
     * @param initData
     * @return
     * @throws Exception
     */
    int newPage(byte[] initData);

    /**
     * 获取数据页
     * @param pgno
     * @return
     * @throws Exception
     */
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);

    void truncateByPgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);

    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                ExceptionHandler.systemStop(Error.FileExistsException);
            }
        } catch (Exception e) {
            ExceptionHandler.systemStop(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            ExceptionHandler.systemStop(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            ExceptionHandler.systemStop(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if(!f.exists()) {
            ExceptionHandler.systemStop(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            ExceptionHandler.systemStop(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            ExceptionHandler.systemStop(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}
