package com.mammb.code.db.query;

public class PredParser {
    private Lexer lexer;

    public PredParser(String s) {
        lexer = new Lexer(s);
    }

    public String field() {
        return lexer.eatId();
    }

    public void constant() {
        if (lexer.matchStringConstant())
            lexer.eatStringConstant();
        else
            lexer.eatIntConstant();
    }

    public void expression() {
        if (lexer.matchId())
            field();
        else
            constant();
    }

    public void term() {
        expression();
        lexer.eatDelimiter('=');
        expression();
    }

    public void predicate() {
        term();
        if (lexer.matchKeyword("and")) {
            lexer.eatKeyword("and");
            predicate();
        }
    }
}
