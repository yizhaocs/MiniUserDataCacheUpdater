package com.yizhao.miniudcu.dataprocessor;

import com.yizhao.miniudcu.clog.CentralLogger;
import com.yizhao.miniudcu.clog.ClogCookieKeyValue;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by yzhao on 4/15/17.
 */
public class CMPrefixDataProcessor extends UDCUDataProcessor {
    private static final Logger log = Logger.getLogger(CMPrefixDataProcessor.class);
    private List<Integer> ignoreNetworks = new ArrayList<Integer>();
    private CentralLogger cookieKeyValueLogger = null;

    public CMPrefixDataProcessor() {
    }

    public void init() {
    }

    public void destroy() {
    }

    public void processData(String[] data, String fileName, int lineNo)
            throws Exception {
        if ((data == null) || (data.length < 5)) {
            log.error("csv file format is wrong. file name: " + fileName + " line number: " + lineNo);

            // here we have a cm line, but we can't understand it. so we will throw an exception and this will
            // cause the file to fail. we will move the file to the ERROR dir and operation will take a look
            throw new IllegalArgumentException("data line has a wrong format. file name: " +
                    fileName + " line number: " + lineNo);
        }

        long cookieId = Long.valueOf(data[2]);
        int keyId = Integer.valueOf(data[3]);
        String value = data[4];

        ClogCookieKeyValue ckvRow = new ClogCookieKeyValue(
                cookieId, keyId,
                value, null, null);
        cookieKeyValueLogger.log(ckvRow);
    }

    @Override
    public void validate(String[] data, String fileName, int lineNo)
            throws Exception {
        // this method is intentionally left blank

    }

}
