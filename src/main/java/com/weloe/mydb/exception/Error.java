package com.weloe.mydb.exception;

public class Error {
    public static Exception BadTranFileException = new BadTranFileException();
    public static FileExistsException FileExistsException = new FileExistsException();
    public static FileCannotRWException FileCannotRWException = new FileCannotRWException();
    public static FileNotExistsException FileNotExistsException = new FileNotExistsException();


    public static Exception CacheFullException = new RuntimeException("缓存已满");
}
class BadTranFileException extends RuntimeException{
    public BadTranFileException() {
        super();
    }

    public BadTranFileException(String message) {
        super(message);
    }
}
class FileExistsException extends RuntimeException{
    public FileExistsException() {
        super();
    }

    public FileExistsException(String message) {
        super(message);
    }
}
class FileCannotRWException extends RuntimeException{
    public FileCannotRWException() {
        super();
    }

    public FileCannotRWException(String message) {
        super(message);
    }
}
class FileNotExistsException extends RuntimeException{
    public FileNotExistsException() {
        super();
    }

    public FileNotExistsException(String message) {
        super(message);
    }
}