package com.yizhao.miniudcu.fileposter;

import com.yizhao.miniudcu.util.FileUtils.FileCreateUtil;
import org.apache.log4j.Logger;

import java.io.File;

/**
 * Created by yzhao on 4/15/17.
 */
public class SimplePosterWorker implements PosterWorker {

    private static final Logger logger = Logger.getLogger(SimplePosterWorker.class);

    private final Poster poster;

    private UrlRouter urlRouter;
    private HttpClient httpClient;
    private int socketTimeoutInMinutes;
    private int connectionTimeoutInSeconds;

    private File errorDir;
    private File archiveDir;

    private boolean enabled = true;


    /* (non-Javadoc)
     * @see com.opinmind.webapp.etlext.PosterWorker#post(java.io.File)
     */
    public void post(File file) throws Exception {

        if(!enabled){
            // if not enabled then don't post the file
            return;
        }

        //choose the host to send file to
        String url = urlRouter.chooseUrl( file.getName() );

        HttpPost httpPost = new HttpPost( url );

        logger.info("posting " + file + " to : " + httpPost.getURI());

        // declare a response entity ( for cleanup in finally clause )
        HttpEntity responseEntity = null;
        try {
            StopWatch chrono = StopWatch.getInstance();
            chrono.startClock();

            FileBody fileBody = new FileBody(file);
            MultipartEntity reqEntity = new MultipartEntity();
            reqEntity.addPart(file.getName(), fileBody);

            httpPost.setEntity(reqEntity);

            logger.info("BEFORE POSTER UPLOAD: " + file + ", size " + file.length() );
            HttpResponse response = httpClient.execute(httpPost);
            int httpStatus = response.getStatusLine().getStatusCode();
            responseEntity = response.getEntity();
            //Bug 7512 - require "OK" in response to confirm that file was successfully received
            String responseBody = IOUtils.toString(responseEntity.getContent(), OpinmindConstants.UTF_8);
            logger.info("AFTER POSTER UPLOAD: " + file + ", size " + file.length() + ", httpStatus: " + httpStatus + ", responseBody[" + responseBody + "]" );

            //SUCCESS only if responseBody starts with OK ( and non-null )
            if (responseBody != null && responseBody.startsWith( "OK" ) ) {
                if (logger.isDebugEnabled()) {
                    logger.info(new StringBuilder().append("Successfully uploaded file ").append(file.getName())
                            .append("; ").append(chrono.stopClockStr()).toString());
                }
            }else{
                //ERROR, throw exception
                String msg = "Upload of file " + file.getName() + " failed: " +
                        "httpStatus[" + httpStatus + "] responseBody[" + responseBody + "]; will retry";
                Exception e = new Exception(msg);
                logger.error(e);
                throw e;
            }

            onSuccess(file,url);
        } catch (Exception e) {
            OpinmindUtil.abort( httpPost ); //httpclient cleanup
            onFailure(file,url);
            throw e;
        } finally {
            OpinmindUtil.consume( responseEntity ); //httpclient cleanup
            poster.donePosting(file);
        }

    }

    private void onFailure(File file, String url) throws Exception {
        urlRouter.recordFailure(url);
        OpinmindUtil.moveFile(file, errorDir);
    }

    private void onSuccess(File file, String url) throws Exception {
        urlRouter.recordSuccess(url);
        OpinmindUtil.moveFile(file, archiveDir);

    }

    PoolingClientConnectionManager connectionManager = null;

    public void init() throws Exception {

        logger.info("Initializing PosterWorkerImpl...");
        Args.validateDefined( urlRouter );
        connectionManager = new PoolingClientConnectionManager();
        connectionManager.setMaxTotal( socketTimeoutInMinutes * 60000 );
        connectionManager.setDefaultMaxPerRoute( connectionTimeoutInSeconds * 1000 );

        httpClient = new DefaultHttpClient( connectionManager );
    }

    public void destroy() throws Exception {
        logger.info("Destroying PosterWorkerImpl...");
        logger.info("Shutting down the PoolingClientConnectionManager...");
        if( connectionManager != null ){
            connectionManager.shutdown();
        }
    }

    public void setUrlRouter(UrlRouter urlRouter) {
        this.urlRouter = urlRouter;
    }

//  /**
//   * @param urlList csv of destination urls for posting files
//   **/
//  public final void setUrl(String urlCsv ) {
//	  this.urlList = CsvUtil.csvToCollection( urlCsv );
//  }

    /**
     * @param connectionTimeoutInSeconds the connectionTimeoutInSeconds to set
     */
    public final void setConnectionTimeoutInSeconds(int connectionTimeoutInSeconds) {
        this.connectionTimeoutInSeconds = connectionTimeoutInSeconds;
    }

    /**
     * @param socketTimeoutInMinutes the socketTimeoutInMinutes to set
     */
    public final void setSocketTimeoutInMinutes(int socketTimeoutInMinutes) {
        this.socketTimeoutInMinutes = socketTimeoutInMinutes;
    }

    /**
     * @param errorDir the errorDir to set
     */
    public final void setErrorDir(String errorDir) throws Exception {
        this.errorDir = new File(errorDir);
        FileCreateUtil.createDirectory(this.errorDir);
    }

    /**
     * @param archiveDir the archiveDir to set
     */
    public final void setArchiveDir(String archiveDir) throws Exception {
        this.archiveDir = new File(archiveDir);
        FileCreateUtil.createDirectory(this.archiveDir);
    }

    public SimplePosterWorker(Poster poster) {
        this.poster = poster;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

}
