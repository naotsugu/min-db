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

    public boolean matchDelimiter(char c) {
        return c == (char) tokenizer.ttype;
    }

    public boolean matchIntConstant() {
        return tokenizer.ttype == StreamTokenizer.TT_NUMBER;
    }

    public boolean matchStringConstant() {
        return '\'' == (char) tokenizer.ttype;
    }

    public boolean matchKeyword(String w) {
        return tokenizer.ttype == StreamTokenizer.TT_WORD &&
            tokenizer.sval.equals(w);
    }

    public boolean matchId() {
        return tokenizer.ttype==StreamTokenizer.TT_WORD &&
            !keywords.contains(tokenizer.sval);
    }

    public void eatDelimiter(char c) {
        if (!matchDelimiter(c)) {
            throw new RuntimeException("syntax error");
        }
        nextToken();
    }

    public int eatIntConstant() {
        if (!matchIntConstant()) {
            throw new RuntimeException("syntax error");
        }
        int i = (int) tokenizer.nval;
        nextToken();
        return i;
    }

    public String eatStringConstant() {
        if (!matchStringConstant()) {
            throw new RuntimeException("syntax error");
        }
        String s = tokenizer.sval; //constants are not converted to lower case
        nextToken();
        return s;
    }

    public void eatKeyword(String w) {
        if (!matchKeyword(w)) {
            throw new RuntimeException("syntax error");
        }
        nextToken();
    }

    public String eatId() {
        if (!matchId()) {
            throw new RuntimeException("syntax error");
        }
        String s = tokenizer.sval;
        nextToken();
        return s;
    }

    private void nextToken() {
        try {
            tokenizer.nextToken();
        } catch (IOException e) {
            throw new RuntimeException("Syntax error");
        }
    }

}
