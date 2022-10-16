package com.weloe.mydb.transaction.impl;

import com.weloe.mydb.exception.Error;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TransactionManagerImplTest {
    TransactionManagerImpl transactionManagerImplUnderTest;
    @BeforeEach
    void setUp() {
        transactionManagerImplUnderTest = Assertions.assertDoesNotThrow(() -> TransactionManagerImpl.open("trantest01"));
    }

    @Test
    void testCreateTranFile() {
        // Setup
        // Run the test
        final TransactionManagerImpl result;
        result = Assertions.assertDoesNotThrow(() -> TransactionManagerImpl.createTranFile("trantest01"));
}

    @Test
    void testOpen() {
        // Setup
        // Run the test

        final TransactionManagerImpl result = Assertions.assertDoesNotThrow(() -> TransactionManagerImpl.open("trantest01"));

        // Verify the results
        //Assertions.assertDoesNotThrow(Error.BadTranFileException.getClass(),()-> result.checkTranIdCounter());
        Assertions.assertDoesNotThrow(()-> result.checkTranIdCounter());
    }


    @Test
    void testCheckTranIdCounter() {
        // Setup
        // Run the test
        final TransactionManagerImpl result;
        result = Assertions.assertDoesNotThrow(() -> TransactionManagerImpl.open("trantest02"));
        // Verify the results
        Assertions.assertDoesNotThrow(()-> result.checkTranIdCounter());

    }

    @Test
    void testClose() {
        // Setup
        // Run the test
        assertDoesNotThrow(() -> transactionManagerImplUnderTest.close());
        // Verify the results
    }

    @Test
    void testStart() {
        // Setup
        // Run the test
        final long tranId = assertDoesNotThrow(() -> transactionManagerImplUnderTest.start());;

        // Verify the results
        assertEquals(1L, tranId);
    }

    @Test
    void testAbort() {
        // Setup
        // Run the test
        assertDoesNotThrow(() -> transactionManagerImplUnderTest.abort(1L));
    }

    @Test
    void testCommit() {
        // Setup
        // Run the test
        Assertions.assertDoesNotThrow(()->transactionManagerImplUnderTest.commit(1L));


        // Verify the results
    }


    @Test
    void testIsAborted() {
        // Setup
        // Run the test
        Boolean result = assertDoesNotThrow(() -> transactionManagerImplUnderTest.isAborted(1L));
        // Verify the results
        assertTrue(result);
    }

    @Test
    void testIsActive() {
        // Setup
        // Run the test
        final boolean result = assertDoesNotThrow(() -> transactionManagerImplUnderTest.isActive(1L));

        // Verify the results
        assertTrue(result);
    }

    @Test
    void testIsCommited() {
        // Setup
        // Run the test
        final boolean result = assertDoesNotThrow(() -> transactionManagerImplUnderTest.isCommited(1L));
        // Verify the results
        assertTrue(result);
    }


}