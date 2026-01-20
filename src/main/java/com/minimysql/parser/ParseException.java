package com.minimysql.parser;

/**
 * ParseException - SQL解析异常
 *
 * 当SQL语法错误或不支持时抛出此异常。
 */
public class ParseException extends RuntimeException {

    public ParseException(String message) {
        super(message);
    }

    public ParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
