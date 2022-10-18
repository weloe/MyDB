package com.weloe.mydb.util;


public class ExceptionHandler {
    public static void handle(Exception e) throws Exception {
        throw e;
    }
    public static void systemStop(Exception e){
        e.printStackTrace();
        System.exit(1);
    }
}
