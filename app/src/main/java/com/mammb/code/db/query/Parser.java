package com.mammb.code.db.query;

import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;

public class Parser {
    private final Lexer lexer;

    Parser(Lexer lexer) {
        this.lexer = lexer;
    }

    public static Parser of(String statement) {
        return new Parser(new Lexer(statement));
    }

    public FieldName field() {
        return FieldName.of(lexer.eatId());
    }

    public DataBox<?> constant() {
        if (lexer.matchStringConstant()) {
            return new DataBox.StrBox(lexer.eatStringConstant());
        } else {
            return new DataBox.IntBox(lexer.eatIntConstant());
        }
    }

    public Expression expression() {
        if (lexer.matchId())
            return new Expression(field());
        else
            return new Expression(constant());
    }

    public Term term() {
        Expression lhs = expression();
        lexer.eatDelimiter('=');
        Expression rhs = expression();
        return new Term(lhs, rhs);
    }

//    public Predicate predicate() {
//        Predicate predicate = new Predicate(term());
//        if (lexer.matchKeyword("and")) {
//            lexer.eatKeyword("and");
//            predicate.conjoinWith(predicate());
//        }
//        return predicate;
//    }
//
//// Methods for parsing queries
//
//    public QueryData query() {
//        lexer.eatKeyword("select");
//        List<String> fields = selectList();
//        lexer.eatKeyword("from");
//        Collection<String> tables = tableList();
//        Predicate pred = new Predicate();
//        if (lexer.matchKeyword("where")) {
//            lexer.eatKeyword("where");
//            pred = predicate();
//        }
//        return new QueryData(fields, tables, pred);
//    }
//
//    private List<String> selectList() {
//        List<String> list = new ArrayList<>();
//        list.add(field());
//        if (lexer.matchDelimiter(',')) {
//            lexer.eatDelimiter(',');
//            list.addAll(selectList());
//        }
//        return list;
//    }
//
//    private Collection<String> tableList() {
//        Collection<String> list = new ArrayList<>();
//        list.add(lexer.eatId());
//        if (lexer.matchDelimiter(',')) {
//            lexer.eatDelimiter(',');
//            list.addAll(tableList());
//        }
//        return list;
//    }
//
//// Methods for parsing the various update commands
//
//    public Object updateCmd() {
//        if (lexer.matchKeyword("insert")) {
//            return insert();
//        } else if (lexer.matchKeyword("delete")) {
//            return delete();
//        } else if (lexer.matchKeyword("update")) {
//            return modify();
//        } else {
//            return create();
//        }
//    }
//
//    private Object create() {
//        lexer.eatKeyword("create");
//        if (lexer.matchKeyword("table")) {
//            return createTable();
//        } else if (lexer.matchKeyword("view")) {
//            return createView();
//        } else {
//            return createIndex();
//        }
//    }
//
//// Method for parsing delete commands
//
//    public DeleteData delete() {
//        lexer.eatKeyword("delete");
//        lexer.eatKeyword("from");
//        String tblname = lexer.eatId();
//        Predicate pred = new Predicate();
//        if (lexer.matchKeyword("where")) {
//            lexer.eatKeyword("where");
//            pred = predicate();
//        }
//        return new DeleteData(tblname, pred);
//    }
//
//// Methods for parsing insert commands
//
//    public InsertData insert() {
//        lexer.eatKeyword("insert");
//        lexer.eatKeyword("into");
//        String tblname = lexer.eatId();
//        lexer.eatDelimiter('(');
//        List<String> flds = fieldList();
//        lexer.eatDelimiter(')');
//        lexer.eatKeyword("values");
//        lexer.eatDelimiter('(');
//        List<DataBox<?>> vals = constList();
//        lexer.eatDelimiter(')');
//        return new InsertData(tblname, flds, vals);
//    }
//
//    private List<String> fieldList() {
//        List<String> list = new ArrayList<>();
//        list.add(field());
//        if (lexer.matchDelimiter(',')) {
//            lexer.eatDelimiter(',');
//            list.addAll(fieldList());
//        }
//        return list;
//    }
//
//    private List<DataBox<?>> constList() {
//        List<DataBox<?>> list = new ArrayList<>();
//        list.add(constant());
//        if (lexer.matchDelimiter(',')) {
//            lexer.eatDelimiter(',');
//            list.addAll(constList());
//        }
//        return list;
//    }
//
//// Method for parsing modify commands
//
//    public ModifyData modify() {
//        lexer.eatKeyword("update");
//        String tblname = lexer.eatId();
//        lexer.eatKeyword("set");
//        String fldname = field();
//        lexer.eatDelimiter('=');
//        Expression newval = expression();
//        Predicate pred = new Predicate();
//        if (lexer.matchKeyword("where")) {
//            lexer.eatKeyword("where");
//            pred = predicate();
//        }
//        return new ModifyData(tblname, fldname, newval, pred);
//    }
//
//// Method for parsing create table commands
//
//    public CreateTableData createTable() {
//        lexer.eatKeyword("table");
//        String tblname = lexer.eatId();
//        lexer.eatDelimiter('(');
//        Schema sch = fieldDefs();
//        lexer.eatDelimiter(')');
//        return new CreateTableData(tblname, sch);
//    }
//
//    private Schema fieldDefs() {
//        Schema schema = fieldDef();
//        if (lexer.matchDelimiter(',')) {
//            lexer.eatDelimiter(',');
//            Schema schema2 = fieldDefs();
//            schema.addAll(schema2);
//        }
//        return schema;
//    }
//
//    private Schema fieldDef() {
//        String fldname = field();
//        return fieldType(fldname);
//    }
//
//    private Schema fieldType(String fldname) {
//        Schema schema = new Schema();
//        if (lexer.matchKeyword("int")) {
//            lexer.eatKeyword("int");
//            schema.addIntField(fldname);
//        }
//        else {
//            lexer.eatKeyword("varchar");
//            lexer.eatDelimiter('(');
//            int strLen = lexer.eatIntConstant();
//            lexer.eatDelimiter(')');
//            schema.addStringField(fldname, strLen);
//        }
//        return schema;
//    }
//
//// Method for parsing create view commands
//
//    public CreateViewData createView() {
//        lexer.eatKeyword("view");
//        String viewname = lexer.eatId();
//        lexer.eatKeyword("as");
//        QueryData qd = query();
//        return new CreateViewData(viewname, qd);
//    }
//
//
////  Method for parsing create index commands
//
//    public CreateIndexData createIndex() {
//        lexer.eatKeyword("index");
//        String idxname = lexer.eatId();
//        lexer.eatKeyword("on");
//        String tblname = lexer.eatId();
//        lexer.eatDelimiter('(');
//        String fldname = field();
//        lexer.eatDelimiter(')');
//        return new CreateIndexData(idxname, tblname, fldname);
//    }

}
