package com.weloe.mydb.constant;


public class TranIdConstant {
    //文件头长度，用于记录事务个数
    public static final int LEN_TRANID_HEADER_LENGTH=8;
    //每个事务占用长度
    public static final int TRANID_FIELD_SIZE =1;
    //执行中
    public static final byte FIELD_TRAN_ACTIVE = 0;
    //已提交
    public static final byte FIELD_TRAN_COMMITTED = 1;
    //事务取消
    public static final byte FIELD_TRAN_ABORTED = 2;
    //超级事务的tranid
    public static final long SUPER_TRANID = 0;
    //文件后缀
    public static final String TRANSACTION_FILE_SUFFIX = ".tranid";


}
