package com.yizhao.miniudcu.util.HttpConnectionUtils;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;

/**
 * Created by yzhao on 4/15/17.
 */
public class HttpClientConnectionUtils {
    /**
     * HttpEntity cleanup ( closes any underlying input streams )
     * httpclient 4.0+
     *
     * @param entity
     */
    public static void consume( HttpEntity entity ){
        if( entity != null ){
            try{
                EntityUtils.consume( entity );
            }catch(Exception e){
                //ignore
            }
        }
    }

    /**
     * abort an http request and close connection
     * will cancel any keepalive
     *
     * httpclient 4.0+
     *
     * @param method
     */
    public static void abort( HttpUriRequest request  ){
        if( request != null ){
            try{
                request.abort();
            }catch(Exception e){
                //ignore
            }
        }
    }
}
