package com.weloe.mydb.data.dataitem;

import com.google.common.primitives.Bytes;
import com.weloe.mydb.common.SubArray;
import com.weloe.mydb.data.DataManagerImpl;
import com.weloe.mydb.data.page.Page;
import com.weloe.mydb.util.ByteParser;
import com.weloe.mydb.util.Types;

import java.util.Arrays;

public interface DataItem {


    SubArray data();

    /**
     * 修改DataItem前调用
     */
    void before();

    /**
     * 要撤销修改时调用
     */
    void unBefore();

    /**
     * 修改完成后调用
     * @param tranId
     */
    void after(long tranId);

    void release();

    void lock();

    void unlock();

    void rLock();

    void rUnLock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();


    /**
     * 从Page的offset处解析dataitem
     * key: 页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节。
     * @param pg
     * @param offset
     * @param dataManager
     * @return
     */
    static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dataManager) {
        byte[] raw = pg.getData();
        short size = ByteParser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(),offset);
        return new DataItemImpl(new SubArray(raw,offset,offset+length),new byte[length],pg,uid,dataManager);
    }

    /**
     * 把DataItem设为无效
     * @param raw
     */
    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }


    static byte[] wrapDataItemRaw(byte[] data) {
        byte[] valid = new byte[1];
        byte[] size = ByteParser.short2Byte((short) data.length);
        return Bytes.concat(valid,size,data);
    }

}
