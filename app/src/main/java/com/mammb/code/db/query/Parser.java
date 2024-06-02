package com.mammb.code.db.query;

import com.mammb.code.db.Schema;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.IdxName;
import com.mammb.code.db.lang.TableName;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

    public Predicate predicate() {
        Predicate predicate = new Predicate(term());
        if (lexer.matchKeyword("and")) {
            lexer.eatKeyword("and");
            predicate.conjoinWith(predicate());
        }
        return predicate;
    }

    public QueryData query() {
        lexer.eatKeyword("select");
        List<FieldName> fields = selectList();
        lexer.eatKeyword("from");
        Collection<TableName> tables = tableList();
        Predicate pred = new Predicate();
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where");
            pred = predicate();
        }
        return new QueryData(fields, tables, pred);
    }

    private List<FieldName> selectList() {
        List<FieldName> list = new ArrayList<>();
        list.add(field());
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            list.addAll(selectList());
        }
        return list;
    }

    private Collection<TableName> tableList() {
        Collection<TableName> list = new ArrayList<>();
        list.add(TableName.of(lexer.eatId()));
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            list.addAll(tableList());
        }
        return list;
    }

    public Object updateCmd() {
        if (lexer.matchKeyword("insert")) {
            return insert();
        } else if (lexer.matchKeyword("delete")) {
            return delete();
        } else if (lexer.matchKeyword("update")) {
            return modify();
        } else {
            return create();
        }
    }

    private Object create() {
        lexer.eatKeyword("create");
        if (lexer.matchKeyword("table")) {
            return createTable();
        } else {
            return createIndex();
        }
    }

    public DeleteData delete() {
        lexer.eatKeyword("delete");
        lexer.eatKeyword("from");
        TableName tableName = TableName.of(lexer.eatId());
        Predicate pred = new Predicate();
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where");
            pred = predicate();
        }
        return new DeleteData(tableName, pred);
    }

    public InsertData insert() {
        lexer.eatKeyword("insert");
        lexer.eatKeyword("into");
        TableName tableName = TableName.of(lexer.eatId());
        lexer.eatDelimiter('(');
        List<FieldName> fields = fieldList();
        lexer.eatDelimiter(')');
        lexer.eatKeyword("values");
        lexer.eatDelimiter('(');
        List<DataBox<?>> vals = constList();
        lexer.eatDelimiter(')');
        return new InsertData(tableName, fields, vals);
    }

    private List<FieldName> fieldList() {
        List<FieldName> list = new ArrayList<>();
        list.add(field());
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            list.addAll(fieldList());
        }
        return list;
    }

    private List<DataBox<?>> constList() {
        List<DataBox<?>> list = new ArrayList<>();
        list.add(constant());
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            list.addAll(constList());
        }
        return list;
    }

    public ModifyData modify() {
        lexer.eatKeyword("update");
        TableName tableName = TableName.of(lexer.eatId());
        lexer.eatKeyword("set");
        FieldName fieldName = field();
        lexer.eatDelimiter('=');
        Expression newVal = expression();
        Predicate pred = new Predicate();
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where");
            pred = predicate();
        }
        return new ModifyData(tableName, fieldName, newVal, pred);
    }

    public CreateTableData createTable() {
        lexer.eatKeyword("table");
        TableName tableName = TableName.of(lexer.eatId());
        lexer.eatDelimiter('(');
        Schema schema = fieldDefs(tableName);
        lexer.eatDelimiter(')');
        return new CreateTableData(schema);
    }

    private Schema fieldDefs(TableName tableName) {
        Schema schema = fieldDef(tableName);
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            Schema schema2 = fieldDefs(tableName);
            schema.addAll(schema2);
        }
        return schema;
    }

    private Schema fieldDef(TableName tableName) {
        FieldName fieldName = field();
        return fieldType(tableName, fieldName);
    }

    private Schema fieldType(TableName tableName, FieldName fieldName) {
        Schema schema = new Schema(tableName);
        if (lexer.matchKeyword("int")) {
            lexer.eatKeyword("int");
            schema.addIntField(fieldName);
        }
        else {
            lexer.eatKeyword("varchar");
            lexer.eatDelimiter('(');
            int strLen = lexer.eatIntConstant();
            lexer.eatDelimiter(')');
            schema.addStringField(fieldName, strLen);
        }
        return schema;
    }

    public CreateIndexData createIndex() {
        lexer.eatKeyword("index");
        IdxName idxName = IdxName.of(lexer.eatId());
        lexer.eatKeyword("on");
        TableName tableName = TableName.of(lexer.eatId());
        lexer.eatDelimiter('(');
        FieldName fieldName = field();
        lexer.eatDelimiter(')');
        return new CreateIndexData(idxName, tableName, fieldName);
    }

}
