package com.yizhao.miniudcu.util.FileUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileCloseUtil {
    /**
     * Closes reader stream.
     *
     * @param reader - Reader stream to be closed
     */
    public static void close(Reader reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
            } // ignore
        }
    }


    /**
     * Closes input stream
     *
     * @param inputStream - input stream that needs to be closed
     */
    public static void close(InputStream inputStream) {
        if (inputStream != null)
            try {
                inputStream.close();
            } catch (Exception e) {
            } // ignore
    }


    /**
     * Closes writer stream after flushing it.
     *
     * @param writer - Writer stream to be closed
     */
    public static void close(Writer writer) {
        if (writer != null) {
            try {
                writer.flush();
            } catch (Exception e) {
            } // ignore

            try {
                writer.close();
            } catch (Exception e) {
            } // ignore
        }
    }

    /**
     * Closes output stream after flushing it.
     *
     * @param outputStream - Writer stream to be closed
     */
    public static void close(OutputStream outputStream) {
        if (outputStream != null) {
            try {
                outputStream.flush();
            } catch (Exception e) {
            } // ignore

            try {
                outputStream.close();
            } catch (Exception e) {
            } // ignore
        }
    }
}
