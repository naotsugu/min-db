package com.mammb.code.db.query;

public class Parser {
    private final Lexer lexer;

    Parser(Lexer lexer) {
        this.lexer = lexer;
    }

    public static Parser of(String statement) {
        return new Parser(new Lexer(statement));
    }

}
