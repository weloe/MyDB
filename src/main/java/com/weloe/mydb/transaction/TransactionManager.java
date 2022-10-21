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
    void abort(long tranId);


    boolean isActive(long tranId);
    boolean isCommited(long tranId);
    boolean isAborted(long tranId);

    /**
     * 关闭事务
     */
    void close() throws Exception;


}
