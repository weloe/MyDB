package com.weloe.mydb.data.logger;

import com.weloe.mydb.exception.Error;
import com.weloe.mydb.util.ByteParser;
import com.weloe.mydb.util.ExceptionHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Logger {
    void log(byte[] data);
    void truncate(long x) throws Exception;

    /**
     * 获取下一条log的data
     * @return
     */
    byte[] next();

    /**
     * 重置日志指针为4
     */
    void rewind();

    void close();

    static Logger create(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
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

        ByteBuffer buf = ByteBuffer.wrap(ByteParser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }

        return new LoggerImpl(raf, fc, 0);
    }

    static Logger open(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
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

        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();

        return lg;
    }

}
