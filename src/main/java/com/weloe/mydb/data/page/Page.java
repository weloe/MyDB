package com.weloe.mydb.data.page;

public interface Page {
    void lock();
    void unlock();

    /**
     * 淘汰当前缓存页
     */
    void release();

    /**
     * 设置为脏页面
     * @param dirty
     */
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
