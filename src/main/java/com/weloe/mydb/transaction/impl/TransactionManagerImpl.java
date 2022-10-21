package com.weloe.mydb.transaction.impl;

import com.weloe.mydb.exception.Error;
import com.weloe.mydb.transaction.TransactionManager;
import com.weloe.mydb.util.ExceptionHandler;
import com.weloe.mydb.util.ByteParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.weloe.mydb.constant.TranIdConstant.*;


public class TransactionManagerImpl implements TransactionManager {

    private RandomAccessFile file;
    private FileChannel fc;

    //TranId的个数
    private long tranIdCounter;

    private Lock counterLock = new ReentrantLock();

    public TransactionManagerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
    }

    @Override
    public long start() throws Exception {
        long tranId = begin();
        return tranId;
    }

    @Override
    public void commit(long tranId) throws Exception {
        updateTranId(tranId, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long tranId) {
        updateTranId(tranId, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long tranId) {
        if (tranId == SUPER_TRANID) {
            return false;
        }
        return checkTranId(tranId, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommited(long tranId) {
        if (tranId == SUPER_TRANID) {
            return false;
        }
        return checkTranId(tranId, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long tranId) {
        if (tranId == SUPER_TRANID) {
            return false;
        }
        return checkTranId(tranId, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() throws Exception {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            ExceptionHandler.handle(e);
        }
    }
    public static TransactionManagerImpl createTranFile(String path) throws Exception {
        File file = new File(path + TRANSACTION_FILE_SUFFIX);

        try {
            boolean newFile = file.createNewFile();
            if(!newFile){
                ExceptionHandler.handle(Error.FileExistsException);
            }
        } catch (IOException e) {
            ExceptionHandler.handle(e);
        }

        if(!file.canRead() || !file.canRead()){
            ExceptionHandler.handle(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            ExceptionHandler.handle(e);
        }

        // 写空文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[LEN_TRANID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            ExceptionHandler.handle(e);
        }

        return new TransactionManagerImpl(raf,fc);
    }

    public static TransactionManagerImpl open(String path) throws Exception {
        File file = new File(path + TRANSACTION_FILE_SUFFIX);
        if(!file.exists()){
            ExceptionHandler.handle(Error.FileNotExistsException);
        }
        if(!file.canRead() || !file.canWrite()){
            ExceptionHandler.handle(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            ExceptionHandler.handle(e);
        }

        return new TransactionManagerImpl(raf,fc);
    }

    /**
     * 检查文件格式
     * 根据文件的前8个字节推出文件length和file.length对比
     */
    public void checkTranIdCounter() throws Exception {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            ExceptionHandler.handle(Error.BadTranFileException);
        }

        if (fileLen < LEN_TRANID_HEADER_LENGTH) {
            ExceptionHandler.handle(Error.BadTranFileException);
        }

        ByteBuffer buffer = ByteBuffer.allocate(LEN_TRANID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buffer);
        } catch (IOException e) {
            ExceptionHandler.handle(e);
        }

        this.tranIdCounter = ByteParser.parseLong(buffer.array());
        long end = getTranIdPosition(this.tranIdCounter + 1);
        if (end != fileLen) {
            ExceptionHandler.handle(Error.BadTranFileException);
        }
    }

    /**
     * 根据tranId得到事务状态的位置
     * @param tranId
     * @return
     */
    private long getTranIdPosition(long tranId) {

        return LEN_TRANID_HEADER_LENGTH + (tranId - 1) * TRANID_FIELD_SIZE;
    }

    /**
     * 开启事务
     *
     * @return tranId
     */
    public long begin() throws Exception {

        long tranId;

        counterLock.lock();
        try {
            tranId = tranIdCounter + 1;
            updateTranId(tranId, FIELD_TRAN_ACTIVE);
            incrTranIdCounter();
            return tranId;
        } finally {
            counterLock.unlock();
        }

    }

    /**
     * 修改tranId事务的状态
     *
     * @param tranId    事务唯一id
     * @param status 事务状态
     */
    private void updateTranId(long tranId, byte status) {
        if(tranId == 0) {
            return;
        }
        long offset = getTranIdPosition(tranId);
        byte[] tranByte = new byte[TRANID_FIELD_SIZE];
        tranByte[0] = status;
        ByteBuffer buffer = ByteBuffer.wrap(tranByte);

        try {
            //从offset开始写
            fc.position(offset);
            fc.write(buffer);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }

        try {
            fc.force(false);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }

    }

    /**
     * 将TranId加一，并更新TranId Header
     */
    private void incrTranIdCounter() throws Exception {
        tranIdCounter++;
        ByteBuffer buffer = ByteBuffer.wrap(ByteParser.long2Byte(tranIdCounter));

        try {
            fc.position(0);
            fc.write(buffer);
        } catch (IOException e) {
            ExceptionHandler.handle(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            ExceptionHandler.handle(e);
        }
    }

    /**
     * 检查事务的状态
     *
     * @param tranId    事务唯一id
     * @param status
     * @return bool
     */
    private boolean checkTranId(long tranId, byte status) {
        long offset = getTranIdPosition(tranId);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[TRANID_FIELD_SIZE]);

        try {
            fc.position(offset);
            fc.read(buffer);
        } catch (IOException e) {
            ExceptionHandler.systemStop(e);
        }

        return buffer.array()[0] == status;
    }

}
