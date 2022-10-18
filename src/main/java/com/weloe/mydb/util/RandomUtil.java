package com.weloe.mydb.util;

import java.security.SecureRandom;

public class RandomUtil {

    /**
     * 随机生成指定长度的字节数组
     * @param lenVc
     * @return
     */
    public static byte[] randomBytes(int lenVc) {
        SecureRandom r = new SecureRandom();
        byte[] bytes = new byte[lenVc];
        r.nextBytes(bytes);
        return bytes;
    }
}
