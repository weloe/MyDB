package com.weloe.mydb.cache;

import com.sun.deploy.panel.JreTableModel;
import com.weloe.mydb.exception.Error;
import com.weloe.mydb.util.ExceptionHandler;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache <T>{
    //缓存的数据
    private HashMap<Long,T> cache;
    //数据的引用个数
    private HashMap<Long,Integer> references;
    //正在被获取的数据
    private HashMap<Long,Boolean> gettingMap;

    //最大缓存数
    private int maxResource;
    //缓存中的个数
    private int count;

    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        gettingMap = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 获取数据
     * @param key
     * @return
     * @throws Exception
     */
    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            if(gettingMap.containsKey(key)){
                //请求的数据正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                }catch (InterruptedException e){
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if (cache.containsKey(key)) {
                //请求的数据在缓存中
                T obj = cache.get(key);
                references.put(key,references.get(key)+1);
                lock.unlock();
                return obj;
            }

            if (maxResource > 0 && count == maxResource) {
                //缓存满
                lock.unlock();
                ExceptionHandler.handle(Error.CacheFullException);
            }

            //缓存的个数加1
            count++;
            //加入正在获取的map
            gettingMap.put(key,true);

            lock.unlock();
            break;
        }

        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            //缓存的个数减1
            count--;

            //从正在获取map删除该key
            gettingMap.remove(key);
            lock.unlock();
            ExceptionHandler.handle(e);
        }

        lock.lock();
        gettingMap.remove(key);
        cache.put(key,obj);
        references.put(key,1);
        lock.unlock();

        return obj;
    }

    /**
     * 淘汰缓存
     */
    protected void release(long key){
        lock.lock();
        try {
            int ref = references.get(key)-1;
            if(ref == 0){
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            }else {
                references.put(key,ref);
            }
        }finally {
            lock.unlock();
        }

    }

    /**
     * 关闭缓存，写回所有资源
     */
    public void close(){
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (Long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } catch (Exception e) {
            lock.unlock();
        }
    }


    /**
     * 当数据不在缓存时的获取数据行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当数据被淘汰时的回调
     */
    protected abstract void releaseForCache(T obj);

}
