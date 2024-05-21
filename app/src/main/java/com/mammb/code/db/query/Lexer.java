package com.mammb.code.db.query;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Collection;
import java.util.List;

public class Lexer {
    private static final Collection<String> keywords = List.of(
        "select", "from", "where", "and",
        "insert", "into", "values", "delete", "update", "set",
        "create", "table", "int", "varchar", "view", "as", "index", "on");
    private final StreamTokenizer tokenizer;

    Lexer(String statement) {
        tokenizer = new StreamTokenizer(new StringReader(statement));
        tokenizer.ordinaryChar('.');   //disallow "." in identifiers
        tokenizer.wordChars('_', '_'); //allow "_" in identifiers
        tokenizer.lowerCaseMode(true); //ids and keywords are converted
        nextToken();
    }

    private void nextToken() {
        try {
            tokenizer.nextToken();
        } catch (IOException e) {
            throw new RuntimeException("Syntax error");
        }
    }

}
