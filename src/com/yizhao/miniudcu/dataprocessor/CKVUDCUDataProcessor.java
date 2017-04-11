package com.yizhao.miniudcu.dataprocessor;

import com.yizhao.miniudcu.cache.UserDataCache;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.util.regex.Pattern;

public class CKVUDCUDataProcessor extends UDCUDataProcessor {
    private static final Logger log = Logger.getLogger(CKVUDCUDataProcessor.class);
    private static final Pattern amperSpliter = Pattern.compile("&");
    private static final Pattern equalSpliter = Pattern.compile("=");
    private UserDataCache cache;
    private DataSource dataSource;

    public CKVUDCUDataProcessor() {
    }

    public void init() {
    }

    public void destroy() {
    }

    @Override
    public void processData(String[] data, String fileName, int lineNo, Boolean shouldWriteClog) throws Exception {

    }

    public UserDataCache getCache() {
        return cache;
    }

    public void setCache(UserDataCache cache) {
        this.cache = cache;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void validate(String[] data, String fileName, int lineNo)
            throws Exception {

    }
}