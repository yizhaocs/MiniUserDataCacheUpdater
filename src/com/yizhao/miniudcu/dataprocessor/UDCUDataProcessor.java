package com.yizhao.miniudcu.dataprocessor;

import com.yizhao.miniudcu.pixeldataengine.PixelDataEngine;

/**
 * This interface represents the data processors for UDCU, they have specific
 * knowledge for the different data types. For example, we will CKVUDCUDataProcessor
 * to process CKV data types in a CKV row.
 */
public abstract class UDCUDataProcessor {
    protected PixelDataEngine pixelDataEngine;

    public PixelDataEngine getPixelDataEngine() {
        return pixelDataEngine;
    }

    public void setPixelDataEngine(PixelDataEngine pixelDataEngine) {
        this.pixelDataEngine = pixelDataEngine;
    }

    /**
     * processes the row data specific to this data type
     *
     * @param data     the current row of data ( the row in the CSV file )
     * @param fileName the file name, for information
     * @param lineNo   the line number of the current line
     * @throws Exception
     */
    abstract public void processData(String[] data, String fileName, int lineNo, Boolean shouldWriteClog) throws Exception;

    abstract public void validate(String[] data, String fileName, int lineNo) throws Exception;

}