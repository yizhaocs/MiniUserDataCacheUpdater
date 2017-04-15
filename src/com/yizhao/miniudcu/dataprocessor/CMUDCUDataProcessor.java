package com.yizhao.miniudcu.dataprocessor;

import com.yizhao.miniudcu.clog.CentralLogger;
import com.yizhao.miniudcu.clog.ClogCookieKeyValue;
import com.yizhao.miniudcu.util.RetryUtil;
import org.apache.log4j.Logger;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by yzhao on 4/15/17.
 */
public class CMUDCUDataProcessor extends UDCUDataProcessor {
    private List<Integer> ignoreNetworks = new ArrayList<Integer>();

    private static final Logger log = Logger.getLogger(CMUDCUDataProcessor.class);

    private CentralLogger cookieKeyValueLogger = null;

    public CMUDCUDataProcessor() {
    }

    public void init() {}

    public void destroy() {}

    @Override
    public void processData(String[] data, String fileName, int lineNo, Boolean shouldWriteClog)
            throws Exception {
        if ( (data == null) || (data.length < 5) ) {
            log.error("csv file format is wrong. file name: " + fileName + " line number: " + lineNo);

            // here we have a cm line, but we can't understand it. so we will throw an exception and this will
            // cause the file to fail. we will move the file to the ERROR dir and operation will take a look
            throw new IllegalArgumentException("data line has a wrong format. file name: " +
                    fileName + " line number: " + lineNo);
        }

        final long cookieId = Long.valueOf(data[2]);
        final String keyValues = data[3];

        ClogCookieKeyValue ckvRow = null;
        if (cookieId != null && newValue != null && timeStamp != null) {
            ckvRow = new ClogCookieKeyValue(
                    cookieId, newKey,
                    newValue, timeStamp, timeStamp);
            cookieKeyValueLogger.log(ckvRow);
        }
    }



    public void setIgnoreNetworks(String ignoreNetworksString) {
        if (ignoreNetworksString!=null && ignoreNetworksString.length()>0) {
            log.info("ignoreNetworksString:" + ignoreNetworksString);
            String[] values = ignoreNetworksString.split(",");
            for (String network: values) {
                ignoreNetworks.add(Integer.valueOf(network.trim()));
            }
        }
    }

    @Override
    public void validate(String[] data, String fileName, int lineNo)
            throws Exception {
        // this method is intentionally left blank

    }

}
