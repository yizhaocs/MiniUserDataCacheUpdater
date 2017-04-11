package com.yizhao.miniudcu.dataprocessor;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by yzhao on 4/11/17.
 */
public class UDCUDataProcessorFactory {
    private final static UDCUDataProcessorFactory _instance = new UDCUDataProcessorFactory();
    private final static Map<String, UDCUDataProcessor> _dataProcessorRegistry = new HashMap<String, UDCUDataProcessor>();

    private UDCUDataProcessorFactory() {
        // don't init
    }

    public static UDCUDataProcessorFactory getInstance() {
        return _instance;
    }

    public UDCUDataProcessor getDataProcessor(String dataType) {
        return _dataProcessorRegistry.get(dataType);
    }

    public void registerDataProcessor(String dataType, UDCUDataProcessor dataProcessor) {
        _dataProcessorRegistry.put(dataType, dataProcessor);
    }
}