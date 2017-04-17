package com.yizhao.miniudcu.util.FileUtils;


import com.yizhao.miniudcu.util.ConstantUtil.Constants;
import com.yizhao.miniudcu.util.ThreadUtils.ThreadSafeDateFormat;
import com.yizhao.miniudcu.util.ThreadUtils.ThreadUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * Created by yzhao on 4/10/17.
 */
public class FileUtil {
    private static final Logger logger = Logger.getLogger(ThreadUtil.class);

    private final static ThreadSafeDateFormat threadSafeDateFormat = new ThreadSafeDateFormat("yyyyMMdd-HHmmss", Constants.UTC);




    /**
     * update the status data structure about the file we just processed.
     * <p>
     * This method is based on the naming convention as following:
     * [PIXELTIMESTAMP].[PIXELSERVERID].[SEQUENCE_NUMBER].[EXTRA].csv
     *
     * @param file the file that we just processed.
     * @throws IOException
     */
    public void updateStatus(Map<String, Long> statusMap, File file) throws IOException {
        FileNameTokenizer fileNameTokenizer = new FileNameTokenizer(file);
        if (fileNameTokenizer.isFileNameValid()) {
            String serverId = fileNameTokenizer.getServerId();

            statusMap.put(serverId, fileNameTokenizer.getSequence());
        } else {
            logger.error("file name: " + file.getName()
                    + " doesn't follow naming convention, "
                    + "we can't update the status map");
        }
    }

    /**
     * move the file out of INBOX directory, and put it in the ARCHIVE directory
     * <p>
     * bug 11287: H2014: archive dir now has hierarchy:  archive/yyyyMMdd/HH/
     *
     * @param file the file to be moved
     */
    public File moveFileToArchiveFolder(Properties configProperties, File file) {
        File newFile = null;

        try {
            String archiveDir = configProperties.getProperty(Constants._ARCHIVE_DIR).trim();

            DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            Date date = new Date();
            String timeStr = dateFormat.format(date);
            String dateHourStr = timeStr.substring(0, 8) + "/" + timeStr.substring(8, 10);

            archiveDir += "/" + dateHourStr;

            if (archiveDir != null) {
                File directory = new File(archiveDir);

                if (!directory.exists()) {
                    directory.mkdirs();
                }
                newFile = new File(archiveDir + "/" + file.getName());
                file.renameTo(newFile);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("successfully moved file : " + file.getName());
            }
        } catch (Exception e) {
            logger.error("move file to archive folder failed", e);
            throw new RuntimeException(e);
        }

        return newFile;
    }

    private final static class FileNameTokenizer {
        private Date timestamp = null;
        private String serverId = null;
        private long sequence = -1;

        private boolean valid = false;

        public FileNameTokenizer(File file) {
            String[] data = file.getName().split("\\.");

            if (data.length >= 4) {
                try {
                    timestamp = threadSafeDateFormat.get().parse(data[0]);
                    serverId = data[1];
                    if (serverId != null) {
                        serverId = serverId.trim().toLowerCase();
                    }
                    sequence = Long.valueOf(data[2]);
                } catch (Exception e) {
                    logger.error("file name format error: " + file.getName(), e);
                }

                if (timestamp != null && serverId != null && sequence != -1)
                    valid = true;
            } else {
                String msg = "file name: " + file.getName()
                        + " doesn't follow naming convention. ";
                logger.error(msg);

                valid = false;
            }
        }

        public boolean isFileNameValid() {
            return valid;
        }

        @SuppressWarnings("unused")
        public Date getTimestamp() {
            return timestamp;
        }

        public String getServerId() {
            return serverId;
        }

        public long getSequence() {
            return sequence;
        }

    }
}