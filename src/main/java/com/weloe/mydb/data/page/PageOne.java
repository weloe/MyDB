package com.weloe.mydb.data.page;

import com.weloe.mydb.data.pagecache.PageCache;
import com.weloe.mydb.util.RandomUtil;

import java.util.Arrays;

/**
 * 数据页第一页
 * 启动时给100~107字节处填入一个随机字节，关闭时将其拷贝到108~115字节
 */
public class PageOne {

    private static final int OF_VC = 100;

    private static final int LEN_VC = 8;

    public static byte[] initRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }
    public static void setVcOpen(Page page){
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    /**
     * 随机生成8个byte存储在100-108
     * @param raw
     */
    private static void setVcOpen(byte[] raw){
        System.arraycopy(RandomUtil.randomBytes(LEN_VC),0,raw,OF_VC,LEN_VC);
    }

    public static void setVcClose(Page page){
        page.setDirty(true);
        setVcClose(page.getData());
    }

    /**
     * 100-108 拷贝到 108-115
     * @param raw
     */
    private static void setVcClose(byte[] raw){
        System.arraycopy(raw,OF_VC,raw,OF_VC+LEN_VC,LEN_VC);
    }

    public static boolean checkVc(Page page){
        return checkVc(page.getData());
    }

    /**
     * 比较100-108和108-116的字节
     * @param raw
     * @return
     */
    private static boolean checkVc(byte[] raw){
        return Arrays.equals(Arrays.copyOfRange(raw,OF_VC,OF_VC+LEN_VC),Arrays.copyOfRange(raw,OF_VC+LEN_VC,OF_VC+2*LEN_VC));
    }

}
