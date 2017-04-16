package com.yizhao.miniudcu.fileposter;

/**
 * UrlRouter chooses a URL.
 * In Gimlet, Used by SimplePosterWorker to decide which loader to send a file to
 *
 * @author heng
 *
 */
public interface UrlRouter {

    public String chooseUrl( String filename );

    //note a successful transmission
    public void recordSuccess( String url );

    //report a failed transmission
    public void recordFailure( String url );

}