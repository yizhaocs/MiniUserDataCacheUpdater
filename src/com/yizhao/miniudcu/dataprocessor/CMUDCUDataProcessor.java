package com.yizhao.miniudcu.dataprocessor;

import org.apache.log4j.Logger;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by yzhao on 4/15/17.
 */
public class CMUDCUDataProcessor extends UDCUDataProcessor {
    private UserDataCache cache;
    private List<Integer> ignoreNetworks = new ArrayList<Integer>();

    private static final Logger log = Logger.getLogger(CMUDCUDataProcessor.class);

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

        if (!OpinmindConstants.UDCU_COOKIE_MAP_TYPE.equals(data[0])) {
            log.error("this is not cm data. file name: " + fileName +
                    " line number: " + lineNo + ". we will skip this line and continue processing");

            // if this line is not cm data, we will log the error and continue process next line
            return;
        }

        // note, according to the contract with the file producer;, the strings are url encoded
        //Date timeStamp = new Date(Long.valueOf(data[1])*1000);
        final Long cookieId = Long.valueOf(data[2]);
        final String externalId = URLDecoder.decode(data[3], OpinmindConstants.UTF_8);
        final Integer networkId = Integer.valueOf(data[4]);

        if (ignoreNetworks.contains(networkId)) {
            // if we want to ignore the network and return
            return;
        }

        if( networkId != null && externalId != null && cookieId != null ){
            RetryUtil.withRetries( new Callable<Void>(){
                                       public Void call() throws Exception {
                                           cache.setCookieIdMapping( networkId, externalId, cookieId );
                                           return null;
                                       }
                                   },
                    "write cookie map to cache failed for cookieId: " + cookieId + ". retrying ",
                    OpinmindConstants.UDCU_RETRY_INTERVAL_MILLISECOND,
                    OpinmindConstants.UDCU_RETRY_TIMES);
        }else{
            throw new IllegalArgumentException(String.format( "invalid cookie_map row: cookie_id[%s], network_id[%s], external_id[%s]",
                    cookieId, networkId, externalId ) );
        }

    }

    public UserDataCache getCache() {
        return cache;
    }

    public void setCache(UserDataCache cache) {
        this.cache = cache;
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
