package com.weloe.mydb.transaction;


public interface TransactionManager {
    /**
     * 开启事务
     * @return
     */
    long start() throws Exception;

    /**
     * 提交事务
     * @param tranId
     */
    void commit(long tranId) throws Exception;

    /**
     * 撤销事务
     * @param tranId
     */
    void abort(long tranId) throws Exception;


    boolean isActive(long tranId) throws Exception;
    boolean isCommited(long tranId) throws Exception;
    boolean isAborted(long tranId) throws Exception;

    /**
     * 关闭事务
     */
    void close() throws Exception;


}