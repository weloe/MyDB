package com.weloe.mydb.data.page;

import com.weloe.mydb.data.pagecache.PageCache;
import com.weloe.mydb.util.ByteParser;

import java.util.Arrays;

/**
 * 除第一页的数据页
 * 以2字节无符号起始
 */
public class PageCommon {
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw,OF_DATA);
        return raw;
    }

    private static void setFSO(byte[] raw,short ofData) {
        System.arraycopy(ByteParser.short2Byte(ofData),0,raw,OF_FREE,OF_DATA);
    }

    //获取page的空闲位置的偏移
    public static short getFSO(Page page){
        return getFSO(page.getData());
    }

    private static short getFSO(byte[] raw) {
        return ByteParser.parseShort(Arrays.copyOfRange(raw,0,2));
    }

    // 将raw插入page，返回插入位置
    public static short insert(Page page,byte[] raw){
        page.setDirty(true);
        short offset = getFSO(page.getData());
        //把raw插入到page.data的第2个字节后面
        System.arraycopy(raw,0,page.getData(),offset,raw.length);
        setFSO(page.getData(),(short) (offset+raw.length));

        return offset;
    }

    /**
     * 获取页面剩余空间大小
     * @param page
     * @return
     */
    public static int getFreeSpace(Page page){
        return PageCache.PAGE_SIZE - (int)getFSO(page.getData());
    }

    //将raw插入page的offset位置，并将page的offset设置为较大的offset
    public static void recoverInsert(Page page,byte[] raw,short offset){
        page.setDirty(true);
        System.arraycopy(raw,0,page.getData(),offset,raw.length);

        short rawFSO = getFSO(page.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(page.getData(), (short) (offset+raw.length));
        }
    }

    //将raw插入page的offset位置，不更新update
    public static void recoverUpdate(Page page,byte[] raw,short offset){
        page.setDirty(true);
        System.arraycopy(raw,0,page.getData(),offset,raw.length);
    }


}
