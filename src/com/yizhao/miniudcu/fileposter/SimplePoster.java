package com.yizhao.miniudcu.fileposter;

import com.yizhao.miniudcu.util.FileUtils.FileReadingUtil;
import com.yizhao.miniudcu.util.FileUtils.FileUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.util.Set;

/**
 * Created by yzhao on 4/15/17.
 */
public final class SimplePoster extends AbstractPoster implements Poster {
    private static final Logger logger = Logger.getLogger(SimplePoster.class);

    /* (non-Javadoc)
     * @see com.opinmind.webapp.fileposter.AbstractPoster#getFiles(java.io.File)
     */
    @Override
    protected File[] getFiles(File dir) throws Exception {
        return dir.listFiles(new FileFilter() {
            public boolean accept(File file) {
                try {
                    if (file.isDirectory()) {
                        return false;
                    }
                    String[] prefixSuffix = FileReadingUtil.getFilePrefixSuffix(file);
                    String prefix = prefixSuffix[0];
                    String suffix = prefixSuffix[1];
                    int index = prefix.indexOf("-");
                    if(index > -1) {
                        prefix = prefix.substring(0, index);
                    }
                    Set<String> validFilePrefixes = getFilePrefixes();
                    if (validFilePrefixes != null && !validFilePrefixes.isEmpty())
                        return (validFilePrefixes.contains(prefix)) && (".csv".equals(suffix));
                    else
                        return ".csv".equals(suffix);

                } catch (Exception e) {
                    logger.warn("Encountered exception while trying to scan file " + file
                            + " for posting; ignoring it for now", e);
                    return false;
                }
            }
        });
    }

}
