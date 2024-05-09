package org.example;

import com.mammb.code.db.DataBase;
import java.nio.file.Path;

public class App {
    public static void main(String[] args) {
        DataBase db = new DataBase(Path.of("db"));
    }
}
