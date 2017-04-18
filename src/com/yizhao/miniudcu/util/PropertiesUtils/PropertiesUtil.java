package com.yizhao.miniudcu.util.PropertiesUtils;

import com.yizhao.miniudcu.util.FileUtils.FileCloseUtil;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Created by yzhao on 4/18/17.
 */
public class PropertiesUtil {
    /**
     * Loads properties from file - can be specified as a Path or a "file://" url
     * On some systems, FileInputStream doesn't appear to handle file:// URL
     * @param fileNameOrURI
     * @return
     * @throws Exception
     */
    public static Properties loadProperties(String fileNameOrURL ) throws Exception {
        InputStream in = null;
        try{
            if( fileNameOrURL.toLowerCase().startsWith("file:")){
                in = new URL(fileNameOrURL).openStream();
            }else{
                in = new FileInputStream(fileNameOrURL);
            }
            Properties props = new Properties();
            props.load(in);
            return props;
        }finally{
            //always close the input stream
            FileCloseUtil.close( in );
        }
    }
}
