package com.yizhao.miniudcu.util.DatabaseUtil;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Static class that manages Datasources, keyed by a string
 * <p/>
 * dependencies:
 * commons-collections-3.1.jar
 * commons-dbcp-1.2.1.jar
 * commons-pool-1.2.jar
 */
public class DatabaseConnectionUtil {
    private static Logger log = Logger.getLogger(DatabaseConnectionUtil.class);

    private static HashMap sources = new HashMap(); //pooled data sources

    private static HashMap params = new HashMap(); //cached connection params

    public static Connection getConnection(String key) {
        try {
            return getSource(key).getConnection();
        } catch (Exception e) {
            log.error("Can't get Connection: ", e);
            return null;
        }
    }

    public static DataSource getSource(String key) {
        return (DataSource) sources.get(key);
    }

    //get connection parameters
    public static Map getParams(String key) {
        return (Map) params.get(key);
    }

    public static DataSource initSourceFromResource(String key, String resourcePath) {
        Properties props = new Properties();
        try {
            props.load(DatabaseConnectionUtil.class.getResourceAsStream(resourcePath));
            return initSource(key, props);
        } catch (Exception e) {
            log.error("Can't load resource: " + resourcePath, e);
            return null;
        }
    }

    public static DataSource initSource(String key, String propFile) {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(propFile));
            return initSource(key, props);
        } catch (Exception e) {
            log.error("Can't load propFile: " + propFile, e);
            return null;
        }
    }

    public static String DEFAULT_JDBC_DRIVER = "com.mysql.jdbc.Driver";
    public static String DEFAULT_JDBC_PROTOCOL = "jdbc:mysql";
    public static String VALIDATION_QUERY = "SELECT 1"; //used to validate connections in pool

    public static DataSource initSource(String key, Map dbProps) {
        boolean success = true;
        //first initialize the driver (default to com.mysql.jdbc.Driver)
        String driver = (String) dbProps.get("driver");
        driver = (driver == null) ? DEFAULT_JDBC_DRIVER : driver.trim();
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            log.error("Can't register driver:" + driver, e);
            return null;
        }

        //user/password always required
        String user = ((String) dbProps.get("user")).trim();
        String password = ((String) dbProps.get("password")).trim();
        int maxConnection = 10; //optional param
        try {
            maxConnection = Integer.parseInt((String) dbProps.get("maxConnection"));
        } catch (Exception e) {
        }


        // 2010-06-01
        // bug : if "url" property is specified, use that as the connect url, verbatim
        //
        String dburl = (String) dbProps.get("url");

        if (dburl != null) {
            log.debug(
                    String.format(
                            "DBPool: Creating data source for: url %s, user %s", dburl, user));
        } else {

            //"url" is not specified, so form it using the other properties ( old behavior )

            String protocol = (String) dbProps.get("protocol");
            protocol = (protocol == null) ? DEFAULT_JDBC_PROTOCOL : protocol.trim();

            String host = ((String) dbProps.get("host")).trim();

            //20071229: Optional parameters
            // - port and database
            //
            String port = ((String) dbProps.get("port"));
            if (port != null) {
                port = port.trim();
            }
            String database = ((String) dbProps.get("database"));
            if (database != null) {
                database = database.trim();
            }

            // port and db name are optional
            // "jdbc:mysql://localhost:3306/some_db?...";
            dburl = protocol + "://" + host;
            if (port != null && port.length() > 0) {
                dburl += ":" + port;
            }
            dburl += "/";
            if (database != null) {
                dburl += database;
            }
            dburl += "?characterEncoding=utf8";

            //2010-06-01: don't put user/pass in JDBC URL!
            //&user=" + user + "&password=" + password;

            //add connection timeout
            dburl += "&connectTimeout=30000";//&socketTimeout=30000";

            //by default, no socket timeout - long delete/insert stmts will cause timeout
            if (System.getProperty("com.opinmind.db.DBPool.socketTimeout") != null) {
                dburl += "&socketTimeout=" + System.getProperty("com.opinmind.db.DBPool.socketTimeout");
            }

            log.debug(
                    String.format(
                            "DBPool: Creating data source for: host %s, db %s, user %s", host, database, user));

        }


        // Now set up the data source (based on sample code from DBCP project)
        //
        // First, we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        GenericObjectPool connectionPool = new GenericObjectPool(null);
        connectionPool.setMaxActive(maxConnection);
        connectionPool.setTestOnBorrow(true); //test the connection when borrowing from pool

        //
        // Next, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(dburl, user, password);

        //
        // Now we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory,
                        connectionPool,
                        null,    //stmtPoolFactory = null
                        VALIDATION_QUERY,
                        false,    //defaultReadOnly = false
                        true);   //default auto-commit = true

        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        PoolingDataSource dataSource = new PoolingDataSource(connectionPool);

        //put the data source in the global map
        sources.put(key, dataSource);

        //cache the connection params also
        params.put(key, dbProps);

        return dataSource;
    }

    public static void main(String[] args) throws Exception {
        String key = "key";
        String propFile = args[0];

        //sample code for using Pool manager

        //1. initialize a DataSource from the properties file
        DataSource ds = DatabaseConnectionUtil.initSource(key, propFile);

        Connection conn = null;
        try {
            //2. get the connection
            conn = ds.getConnection();

            //3. do stuff: create/execute statements, commit, rollback, etc...
            System.out.println("CONNECTION: " + conn);
        } finally {
            //4. always close the connection
            DatabaseCloseUtil.closeConnection(conn);

            //NOTE: don't need to "return" the connection to the pool explicitly.
            //conn.close() is a wrapper that will return the conn to the pool automatically.

        }

    }
}
