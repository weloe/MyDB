package com.weloe.mydb.util;

import com.weloe.mydb.exception.Error;
import org.junit.jupiter.api.Test;


class ExceptionHandlerTest {

    @Test
    void handle() throws Exception {
        ExceptionHandler.handle(Error.BadTranFileException);
    }
}