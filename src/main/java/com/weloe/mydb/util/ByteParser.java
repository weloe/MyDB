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
     * long转byte
     * @param val
     * @return
     */
    public static byte[] long2Byte(long val) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(val).array();
    }
}
