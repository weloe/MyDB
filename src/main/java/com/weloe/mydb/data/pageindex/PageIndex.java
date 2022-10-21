package com.weloe.mydb.data.pageindex;

import com.weloe.mydb.data.page.Page;
import com.weloe.mydb.data.pagecache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    // 将一页划分成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private List<PageInfo>[] lists;
    private Lock lock;

    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }

    }


    /**
     * 插入pageIndex
     * @param pageNo
     * @param freeSpace
     */
    public void add(int pageNo,int freeSpace){
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pageNo,freeSpace));
        }finally {
            lock.unlock();
        }

    }


    /**
     * 获取pageInfo
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO) {
                number++;
            }

            while (number <= INTERVALS_NO) {
                if(lists[number].size() == 0){
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        }finally {
            lock.unlock();
        }

    }



}
