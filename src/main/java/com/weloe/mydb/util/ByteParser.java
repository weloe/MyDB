package com.weloe.mydb.util;

import java.nio.ByteBuffer;

public class ByteParser {
    /**
     * 把字节转化为long
     * @param buf
     * @return 大小
     */
    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    /**
     * long转byte[]
     * @param val
     * @return
     */
    public static byte[] long2Byte(long val) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(val).array();
    }


    /**
     * short转byte[]
     * @param val
     * @return
     */
    public static byte[] short2Byte(short val) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(val).array();
    }

    public static short parseShort(byte[] buffer) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, 2);
        return byteBuffer.getShort();
    }

    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int parseInt(byte[] array) {
        ByteBuffer buffer = ByteBuffer.wrap(array, 0, 4);
        return buffer.getInt();
    }
}
