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

    private static final String[] fileExts = new String[]{".csv", ".force"};


    /**
     * get the next file.
     * <p>
     * we define the next file to be the file with the smallest timestamp in the
     * data dir
     * <p>
     * we assume the file name follows a naming convention:
     * [PIXELTIMESTAMP].[PIXELSERVERID].[SEQUENCE_NUMBER].[EXTRA].csv
     *
     * @return the qualified file null if we can't find one
     */
    public File getNextFile(Properties configProperties, Map<String, Boolean> processingFiles) {
        File nextFile = null;

        // we only process .csv files and .force files
        // .csv files are data files
        // .force files are manually moved back from the 'error' dir by
        // operation
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                for (String fileExt : fileExts) {
                    if (name.endsWith(fileExt)) {
                        return true;
                    }
                }

                return false;
            }
        };

        File directory = new File(configProperties.getProperty(Constants._INBOX_DIR).trim());

        synchronized (processingFiles) {
            File[] files = directory.listFiles(filter);

            // if inbox directory doesn't exist or is empty, we will return null
            if (files != null) {
                if (files.length > 0) {

                    // sort the dir by their timestamp
                    Arrays.sort(files, new Comparator<File>() {
                        public int compare(File f1, File f2) {
                            return f1.getName().compareTo(f2.getName());
                        }
                    });

                    // we will always get the first non-processing file to
                    // process
                    for (File file : files) {
                        if (processingFiles.get(file.getName()) == null) {
                            nextFile = file;

                            processingFiles.put(file.getName(), Boolean.TRUE);

                            break;
                        }
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("directory might be empty : " + directory);
                    }
                }
            } else {
                logger.info("directory might not exist : " + directory);
            }
        }

        return nextFile;
    }

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