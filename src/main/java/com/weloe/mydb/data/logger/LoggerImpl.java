package com.weloe.mydb.data.logger;

import com.google.common.primitives.Bytes;
import com.weloe.mydb.exception.Error;
import com.weloe.mydb.util.ByteParser;
import com.weloe.mydb.util.ExceptionHandler;

import javax.security.auth.kerberos.KerberosTicket;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件 [XCheckSum] [Log1] [Log2] ...
 *              4       4
 * 每条日志Log为 [size] [CheckSum] [Data]
 */
public class LoggerImpl implements Logger{
    private static final int SEED = 13331;
    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    void init(){
        long size = 0;

        try {
            size = file.length();
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }

        if (size < 4) {
            ExceptionHandler.systemStop(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);

        try {
            //读xCheckSum
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }
        int xCheckSum = ByteParser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xCheckSum;

        checkAndRemoveTail();
    }

    // 检查并移除bad tail
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while (true){
            byte[] log = internNext();
            if(log == null) {
                break;
            }
            xCheck = calChecksum(xCheck,log);
        }
        if(xCheck != xChecksum) {
            ExceptionHandler.systemStop(Error.BadLogFileException);
        }

        try {
            truncate(position);
        } catch (Exception e) {
            ExceptionHandler.systemStop(e);
        }

        try {
            file.seek(position);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }

        rewind();
    }

    private int calChecksum(int xCheck, byte[] log){
        for (byte b :
                log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    private byte[] internNext(){
        if(position + OF_DATA >= fileSize){
            return null;
        }

        //读取size
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }
        int size = ByteParser.parseInt(tmp.array());
        if (position + size + OF_DATA > fileSize) {
            return null;
        }

        //读取checksum+data
        ByteBuffer buffer = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buffer);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }
        byte[] log = buffer.array();

        //校验checksum
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log,OF_DATA,log.length));
        int checkSum2 = ByteParser.parseInt(Arrays.copyOfRange(log,OF_CHECKSUM,OF_DATA));
        if(checkSum1 != checkSum2){
            return null;
        }
        position += log.length;
        return log;
    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buffer = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buffer);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        } finally {
            lock.unlock();
        }
        //更新校验和
        updateXCheckSum(log);
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checkSum = ByteParser.int2Byte(calChecksum(0,data));
        byte[] size = ByteParser.int2Byte(data.length);
        return Bytes.concat(size,checkSum,data);
    }

    /**
     * 更新校验和
     * @param log
     */
    private void updateXCheckSum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum,log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(ByteParser.int2Byte(xChecksum)));
            //刷新缓冲区
            fc.force(false);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }

    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null){
                return null;
            }
            return Arrays.copyOfRange(log,OF_DATA,log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }
    }
}
