package com.yizhao.miniudcu.util.DatabaseUtil;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by yzhao on 4/17/17.
 */
public class DatabaseCloseUtil {
    private static final Logger log = Logger.getLogger(DatabaseCloseUtil.class);
    //
    // Convenience methods for closing JDBC resources
    //
    public static void close( ResultSet rs ) {
        closeResultSet( rs );
    }
    public static void close( Statement stmt ) {
        closeStatement( stmt );
    }
    public static void close( Connection conn ) {
        closeConnection( conn );
    }
    public static void close( ResultSet rs, Statement stmt, Connection conn ){
        close( rs );
        close( stmt );
        close( conn );
    }

    public static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e ) {
                log.error( "closeResultSet()", e );
            }
        }
    }

    public static void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (Exception e) {
                log.error( "closeStatement()", e );
            }
        }
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                log.error( "closeConnection()", e );
            }
        }
    }
}
