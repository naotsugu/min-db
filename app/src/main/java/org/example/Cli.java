package org.example;

import com.mammb.code.db.jdbc.EmbeddedDriver;
import java.sql.*;
import java.util.Scanner;

public class Cli {
    private static final System.Logger log = System.getLogger(Cli.class.getName());

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Connect> ");
        String s = sc.nextLine();
        Driver d = new EmbeddedDriver();

        try (Connection conn = d.connect(s, null);
             Statement stmt = conn.createStatement()) {
            System.out.print("\nSQL> ");
            while (sc.hasNextLine()) {
                // process one line of input
                String cmd = sc.nextLine().trim();
                if (cmd.startsWith("exit")) {
                    break;
                } else if (cmd.startsWith("select")) {
                    doQuery(stmt, cmd);
                } else {
                    doUpdate(stmt, cmd);
                }
                System.out.print("\nSQL> ");
            }
        } catch (SQLException e) {
            log.log(System.Logger.Level.ERROR, e);
        }
        sc.close();
    }

    private static void doQuery(Statement stmt, String cmd) {
        try (ResultSet rs = stmt.executeQuery(cmd)) {
            ResultSetMetaData md = rs.getMetaData();
            int numCols = md.getColumnCount();
            int totalwidth = 0;

            // print header
            for(int i = 1; i <= numCols; i++) {
                String fieldName = md.getColumnName(i);
                int width = md.getColumnDisplaySize(i);
                totalwidth += width;
                String fmt = "%" + width + "s";
                System.out.format(fmt, fieldName);
            }
            System.out.println();
            for (int i = 0; i < totalwidth; i++) {
                System.out.print("-");
            }
            System.out.println();

            // print records
            while (rs.next()) {
                for (int i = 1; i <= numCols; i++) {
                    String fieldName = md.getColumnName(i);
                    int fieldType = md.getColumnType(i);
                    String fmt = "%" + md.getColumnDisplaySize(i);
                    if (fieldType == Types.INTEGER) {
                        System.out.format(fmt + "d", rs.getInt(fieldName));
                    } else {
                        System.out.format(fmt + "s", rs.getString(fieldName));
                    }
                }
                System.out.println();
            }
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }
    }

    private static void doUpdate(Statement stmt, String cmd) {
        try {
            int ret = stmt.executeUpdate(cmd);
            System.out.println(ret + " records processed");
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }
    }

}
