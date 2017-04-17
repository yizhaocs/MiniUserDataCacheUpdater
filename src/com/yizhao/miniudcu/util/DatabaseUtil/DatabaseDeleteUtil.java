package com.yizhao.miniudcu.util.DatabaseUtil;

import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/**
 * Created by yzhao on 4/17/17.
 */
public class DatabaseDeleteUtil {
    private static final Logger log = Logger.getLogger(DatabaseDeleteUtil.class);
    /**
     * Delete all rows from a table.  Useful for unit tests that operate on tables
     * @return # of rows deleted
     */
    public static int deleteTableRows( String dbKey, String tableName ) throws Exception {
        return deleteTableRows( DatabaseConnectionUtil.getSource( dbKey ), tableName );
    }

    /**
     * Delete all rows from a table.  Useful for unit tests that operate on tables
     * @return # of rows deleted
     */
    public static int deleteTableRows(DataSource ds, String tableName ) throws Exception {
        return deleteRows( ds, tableName, null, null, null );
    }

    /**
     * Enforce the maxRowsAllowed in a table by deleting extra rows.
     * orderBy specifies the deletion order ( i.e. which rows to delete first )
     *
     * If the table contains fewer rows than the maxRowsAllowed, then nothing is deleted.
     *
     * @param ds DataSource
     * @param tableName table name
     * @param maxRowsAllowed maximum # of rows to keep in the table
     * @param orderBy (optional) delete rows in the specified order, e.g. "columnB DESC"
     * @return # of rows deleted
     * @throws Exception
     */
    public static int deleteRowsToSize( DataSource ds,
                                        String tableName,
                                        long maxRowsAllowed,
                                        String orderBy ) throws Exception {

        long count = DatabaseReadingUtil.countRows( ds, tableName );

        long numToDelete = count - maxRowsAllowed;

        if( numToDelete > 0 ){
            return deleteRows( ds, tableName, null, orderBy, numToDelete );
        }else{
            log.info( "No need to delete, count = " + count + ", maxRowsAllowed = " + maxRowsAllowed );
            return 0;
        }
    }


    /**
     * Delete rows from a table subject to optional conditions
     * @param ds DataSource
     * @param tableName table name
     * @param where (optional) only delete rows that satisfy the condition, e.g. "columnA < 5"
     * @param orderBy (optional) delete rows in the specified order, e.g. "columnB DESC"
     * @param limit (optional) delete at most this many rows
     * @return # of rows deleted
     * @throws Exception
     */
    public static int deleteRows( DataSource ds,
                                  String tableName,
                                  String where,
                                  String orderBy,
                                  Long limit ) throws Exception {

        String sql = "DELETE FROM " + tableName;

        if( where != null && where.trim().length() > 0 ){
            sql += " WHERE " + where;
        }

        if( orderBy != null && orderBy.trim().length() > 0){
            sql += " ORDER BY " + orderBy;
        }

        if( limit != null ){
            sql += " LIMIT " + limit;
        }

        log.info( "DELETING ROWS: " + sql );

        Connection conn = null;
        Statement stmt = null;

        try{

            conn = ds.getConnection();
            stmt = conn.createStatement();
            return stmt.executeUpdate( sql );

        }catch( Exception e ){
            log.error( "deleteRows()", e );
            throw e;
        }finally{
            DatabaseCloseUtil.closeStatement( stmt );
            DatabaseCloseUtil.closeConnection( conn );
        }

    }
}
