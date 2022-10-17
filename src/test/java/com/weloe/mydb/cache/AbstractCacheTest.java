package com.weloe.mydb.cache;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AbstractCacheTest {
    AbstractCache<String> abstractCacheUnderTest;

    @BeforeEach
    void setUp() {
        abstractCacheUnderTest = new AbstractCache<String>(1) {
            @Override
            protected String getForCache(long key) throws Exception {
                System.out.println("getForCache");
                return "123";
            }

            @Override
            protected void releaseForCache(String obj) {
                System.out.println("releaseForCache");
                return;
            }
        };
    }

    @Test
    public void testClose() {
        // Setup
        // Run the test
        Assertions.assertDoesNotThrow(()->abstractCacheUnderTest.close());
        // Verify the results
    }

    @Test
    public void testGet() {
        // Setup
        // Run the test
        final String result = Assertions.assertDoesNotThrow(()->abstractCacheUnderTest.get(0L));

        // Verify the results
        assertEquals("123", result);
    }

    @Test
    public void testGet_ThrowsException() {
        // Setup
        // Run the test
        final String result = Assertions.assertDoesNotThrow(()->abstractCacheUnderTest.get(0L));

        // Verify the results
        assertEquals("123", result);
        Assertions.assertThrows(Exception.class,()->{
            abstractCacheUnderTest.get(1L);
        });

    }

    @Test
    public void testRelease() {
        // Setup
        // Run the test
        final String result = Assertions.assertDoesNotThrow(()->abstractCacheUnderTest.get(0L));

        // Verify the results
        assertEquals("123", result);
        Assertions.assertDoesNotThrow(()-> abstractCacheUnderTest.release(0L));


        // Verify the results
    }
}