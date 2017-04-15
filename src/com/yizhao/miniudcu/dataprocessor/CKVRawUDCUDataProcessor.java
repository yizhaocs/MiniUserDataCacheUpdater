package com.yizhao.miniudcu.dataprocessor;

import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by yzhao on 4/15/17.
 */
public class CKVRawUDCUDataProcessor  extends CKVUDCUDataProcessor {
    private static final Logger log = Logger.getLogger(CKVRawUDCUDataProcessor.class);

    private CentralLogger cookieKeyValueLogger = null;
    private CentralLogger eventKeyValueLogger = null;
    private CentralLogger conversionLogLogger = null;
    private CentralLogger conversionExtensionLogger = null;

    private DataSource dataSource = null;

    private Map<String, Double> currencyExchangeRates = new HashMap<String, Double>();

    private int refreshInterval = 60; // seconds

    public final static String CONVERSION_LOG = "conversion_log";
    public final static String CONVERSION_EXTENSION = "conversion_extension";

    private final ScheduledExecutorService refreshThread = Executors
            .newSingleThreadScheduledExecutor();

    public void init() {
        currencyExchangeRatesRefresh();
        refreshThread.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                Thread.currentThread().setName("UDCUFileHelper");
                try {
                    currencyExchangeRatesRefresh();
                } catch (Exception e) {
                    log.error("UDCUFileHelper.refresh:", e);
                }

            }
        }, refreshInterval, refreshInterval, TimeUnit.SECONDS);
    }

    public void destroy() {
        refreshThread.shutdownNow();
    }


    /**
     * process the key value pair
     *
     * input key and value
     * output keyValuesMap
     *
     * @param keyValuesMap
     * @param timeStamp
     * @param cookieId
     * @param eventIdStr
     * @param dpIdStr
     * @param locationIdStr
     * @param keysGoToEkv
     * @param keysGoToCkv
     * @param keysGoToBidgen
     * @param shouldWriteClog
     * @return
     * @throws Exception
     */
    @Override
    protected Map<Integer,KeyValueTs> processKeyValue(
            Map<String, String> keyValuesMap,
            Date timeStamp,
            Long cookieId,
            String eventIdStr,
            String dpIdStr,
            String locationIdStr,
            Set<Integer> keysGoToEkv,
            Set<Integer> keysGoToCkv,
            Set<Integer> keysGoToBidgen,
            Boolean shouldWriteClog) throws Exception {
        Map<Integer,KeyValueTs> ckvMap = new HashMap<Integer, KeyValueTs>();

        // call the pixel data engine
        Map<String, Map<String, String>> resultMap = invokePixelDataEngine(keyValuesMap);

        if (resultMap != null && !resultMap.isEmpty()) {
            Map<String, String> defaultTargetMapResult = resultMap.get(PixelDataEngine.DEFAULT_TARGET);

            Map<String, String> validMap = validatePdeResults(defaultTargetMapResult);

            if(validMap != null && !validMap.isEmpty()) {
                ckvMap = processCkvData(validMap,
                        timeStamp,
                        cookieId,
                        eventIdStr,
                        dpIdStr,
                        locationIdStr,
                        keysGoToEkv,
                        keysGoToCkv,
                        keysGoToBidgen,
                        shouldWriteClog);
            }

            Map<String, String> conversionLogMapResult = resultMap.get(CONVERSION_LOG);
            Map<String, String> conversionExtensionMapResult = resultMap.get(CONVERSION_EXTENSION);

            // right now we also support conversion_log and conversion_extension tables
            if(conversionLogMapResult != null && !conversionLogMapResult.isEmpty()) {
                processConversionLog(conversionLogMapResult, keyValuesMap, cookieId, ConversionServerConstants.CONVERSION_REQUEST_PREFIX, shouldWriteClog);
            }

            if(conversionExtensionMapResult != null && !conversionExtensionMapResult.isEmpty()) {
                processConversionExtension(conversionExtensionMapResult, keyValuesMap, cookieId, ConversionServerConstants.CONVERSION_REQUEST_PREFIX, shouldWriteClog);
            }
        }

        return ckvMap;
    }

    /**
     * here we invoke the pixel data engine in inclusive mode
     * ckvraw data is dpkey based, we want the data even if there is no rule defined for it
     *
     * @param originalMap
     * @return
     */
    protected Map<String, Map<String, String>> invokePixelDataEngine(Map<String, String> originalMap) {
        if(originalMap == null || originalMap.isEmpty()){
            return new HashMap<String, Map<String, String>>	();
        }

        // invoke pde
        Map<String, Map<String, String>> resultMap = pixelDataEngine.processRule(originalMap);

        // ckv map
        Map<String, String> ckvMap = new HashMap<String, String>();
        if(!originalMap.isEmpty()) {
            ckvMap.putAll(originalMap);
        }

        Map<String, String> defaultTargetMapResult = resultMap.get(PixelDataEngine.DEFAULT_TARGET);
        if (defaultTargetMapResult!= null && !defaultTargetMapResult.isEmpty()) {
            ckvMap.putAll(defaultTargetMapResult);
        }

        if(!ckvMap.isEmpty()) {
            resultMap.put(PixelDataEngine.DEFAULT_TARGET, ckvMap);
        }

        // conversion log
        Map<String, String> convLogMap = new HashMap<String, String>();
        if(!originalMap.isEmpty()) {
            convLogMap.putAll(originalMap);
        }

        Map<String, String> conversionLogMapResult = resultMap.get(CONVERSION_LOG);
        if (conversionLogMapResult != null && !conversionLogMapResult.isEmpty()) {
            convLogMap.putAll(conversionLogMapResult);
        }

        if(!convLogMap.isEmpty()) {
            resultMap.put(CONVERSION_LOG, convLogMap);
        }

        // conversion extension
        Map<String, String> convExtMap = new HashMap<String, String>();
        if(!originalMap.isEmpty()) {
            convExtMap.putAll(originalMap);
        }

        Map<String, String> conversionExtensionMapResult = resultMap.get(CONVERSION_EXTENSION);
        if (conversionExtensionMapResult != null && !conversionExtensionMapResult.isEmpty()) {
            convExtMap.putAll(conversionExtensionMapResult);
        }

        if(!convExtMap.isEmpty()){
            resultMap.put(CONVERSION_EXTENSION, convExtMap);
        }


        return resultMap;
    }

    /**
     * valid the keys in the map is all numeric
     *
     * @param resultMap
     * @return
     */
    private Map<String, String> validatePdeResults(Map<String, String> resultMap) {
        Map<String, String> validMap = new HashMap<String, String>();

        for (String keyStr : resultMap.keySet()) {
            Integer key = null;
            try {
                key = Integer.valueOf(keyStr);
            } catch (NumberFormatException e) {
                log.warn("validatePdeResults: key : " + keyStr + " is not numeric");
            }

            if (key != null) {
                validMap.put(keyStr, resultMap.get(keyStr));
            }
        }

        return validMap;
    }

    /**
     * handles the ckv data - the ckv data is under the target "default"
     *
     * @param targetMap
     * @param timeStamp
     * @param cookieId
     * @param eventIdStr
     * @param dpIdStr
     * @param locationIdStr
     * @param keysGoToEkv
     * @param keysGoToCkv
     * @param keysGoToBidgen
     * @param shouldWriteClog
     * @return
     */
    private Map<Integer,KeyValueTs> processCkvData(Map<String, String> targetMap,
                                                   Date timeStamp,
                                                   Long cookieId,
                                                   String eventIdStr,
                                                   String dpIdStr,
                                                   String locationIdStr,
                                                   Set<Integer> keysGoToEkv,
                                                   Set<Integer> keysGoToCkv,
                                                   Set<Integer> keysGoToBidgen,
                                                   Boolean shouldWriteClog) {
        Map<Integer,KeyValueTs> ckvMap = new HashMap<Integer, KeyValueTs>();

        // handle the ckv data, the ckv data is under the target "default"
        if(targetMap != null) {
            for (String newKeyStr : targetMap.keySet()) {
                int newKey = Integer.valueOf(newKeyStr);
                String newValue = targetMap.get(newKeyStr);

                // truncate the newValue if length exceeds 80 chars.
                if (newValue != null && newValue.length() > 80) {
                    newValue = newValue.substring(0, 80);
                }

                // only put it in teh keyValuesMap ( to be written to bidgen ) if the key is bidgen key
                if (keysGoToBidgen == null || keysGoToBidgen.contains(newKey)) {
                    KeyValueTs kvtInFile = new KeyValueTs(newKey, newValue, timeStamp);
                    ckvMap.put(newKey, kvtInFile);
                }

                if (shouldWriteClog) {
                    // netezza ckv clogging
                    if (keysGoToCkv == null || keysGoToCkv.contains(newKey)) {
                        ClogCookieKeyValue ckvRow = null;
                        if (cookieId != null && newValue != null && timeStamp != null) {
                            ckvRow = new ClogCookieKeyValue(
                                    cookieId, newKey,
                                    newValue, timeStamp, timeStamp);
                            cookieKeyValueLogger.log(ckvRow);
                        }

                        if (log.isDebugEnabled())
                            log.debug("ckv after pde - cookieId:" + ckvRow);
                    }

                    // netezza ekv clogging
                    if (keysGoToEkv == null || keysGoToEkv.contains(newKey)) {
                        EventKeyValue ekvRow = null;

                        if (eventIdStr != null && !"null".equalsIgnoreCase(eventIdStr) &&
                                newValue != null && !"null".equalsIgnoreCase(newValue)) {
                            // netezza ekv clogging
                            Long eventId = Long.valueOf(eventIdStr);
                            Integer dpId = (dpIdStr == null || "null".equalsIgnoreCase(dpIdStr)) ? null : Integer.valueOf(dpIdStr);
                            Integer locationId = (locationIdStr == null || "null".equalsIgnoreCase(locationIdStr)) ? null : Integer.valueOf(locationIdStr);
                            ekvRow = new EventKeyValue(eventId, newKey, newValue, cookieId, dpId, locationId);

                            eventKeyValueLogger.log(ekvRow);
                        }

                        if (log.isDebugEnabled()) {
                            log.debug("ekv after pde - " + ekvRow);
                        }
                    }
                }
            }

        }

        return ckvMap;
    }

    /**
     * handles the conversion_log data, the conversion_log data should be under the "conversion_log" target
     * this should be configured in the pde rule (pixel_data_engine_configs.target column)
     *
     * Note, all the parameter names are the same as the request parameter names
     *
     * @param resultMap
     * @param shouldWriteClog
     */
    private void processConversionLog(Map<String, String> targetMap,
                                      Map<String, String> originalMap,
                                      Long cookieId,
                                      String paramPrefix,
                                      Boolean shouldWriteClog) {
        // create the conversion log from the pde output
        if (Boolean.TRUE.equals(shouldWriteClog)) {
            String id = targetMap.get(ConversionServerConstants.ID);
            if (id == null)
                id = targetMap.get(ConversionServerConstants.PIXEL_REQUEST_ID);

            String conversionTs = targetMap.get(ConversionServerConstants.CONVERSION_TS);
            if (conversionTs == null)
                conversionTs = targetMap.get(ConversionServerConstants.REQUEST_TS);

            String advertiserId = targetMap.get(paramPrefix+HTTP_PARAM.ADVERTISER_ID);
            String conversionPixelId = targetMap.get(paramPrefix+HTTP_PARAM.CONVERSION_PIXEL_ID);

            String userIpAddress = targetMap.get(ConversionServerConstants.USER_IP_ADDRESS);
            String refererUrl = targetMap.get(ConversionServerConstants.REFERER_URL);

            String advertiserTransactionId = targetMap.get(paramPrefix+HTTP_PARAM.ADVERTISER_TRANSACTION_ID);
            String totalPayout = targetMap.get(paramPrefix+HTTP_PARAM.TOTAL_PAYOUT);

            String userId = targetMap.get(paramPrefix+HTTP_PARAM.USER_ID);
            String locationId = targetMap.get(ConversionServerConstants.LOCATION_ID);
            String impressionId = targetMap.get(paramPrefix+HTTP_PARAM.IMPRESSION_ID);
            String userIdType = targetMap.get(paramPrefix+HTTP_PARAM.USER_ID_TYPE);

            if (id!=null && conversionTs!=null && advertiserId!=null && conversionPixelId!=null && cookieId!=null) {
                ConversionLog conversionLog = new ConversionLog(
                        Long.valueOf(Long.valueOf(id)),
                        new Date(Long.valueOf(conversionTs)),
                        Integer.valueOf(advertiserId),
                        Integer.valueOf(conversionPixelId),
                        cookieId,
                        userIpAddress==null?null:Long.valueOf(userIpAddress),
                        refererUrl,
                        advertiserTransactionId,
                        totalPayout==null?null:Long.valueOf(totalPayout),
                        userId,
                        locationId==null?null:Integer.valueOf(locationId),
                        impressionId==null?null:Long.valueOf(impressionId),
                        userIdType==null?null:Short.valueOf(userIdType),
                        Boolean.TRUE);

                conversionLogLogger.log(conversionLog);
            }
        }
    }

    /**
     * handles the conversion_log data, the conversion_log data should be under the "conversion_extension" target
     * this should be configured in the pde rule (pixel_data_engine_configs.target column)
     *
     * Note, all the parameter names are the same as the request parameter names
     *
     * @param resultMap
     * @param shouldWriteClog
     */
    private void processConversionExtension(Map<String, String> targetMap,
                                            Map<String, String> originalMap,
                                            Long cookieId,
                                            String paramPrefix,
                                            Boolean shouldWriteClog) {
        // create conversion extension from pde output
        if (Boolean.TRUE.equals(shouldWriteClog)) {
            // id
            String id = targetMap.get(ConversionServerConstants.ID);
            if (id == null)
                id = targetMap.get(ConversionServerConstants.PIXEL_REQUEST_ID);

            // conversion_pixel_id
            String conversionPixelId = targetMap.get(paramPrefix+HTTP_PARAM.CONVERSION_PIXEL_ID);

            // conversion_ts
            String conversionTs = targetMap.get(ConversionServerConstants.CONVERSION_TS);
            if (conversionTs == null)
                conversionTs = targetMap.get(ConversionServerConstants.REQUEST_TS);

            // general fields
            String activityTypeStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.ACTIVITY_TYPE);
            Integer activityType = activityTypeStr==null?null:Integer.valueOf(activityTypeStr);
            String currencyType = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.CURRENCY_TYPE);

            // hotel fields
            String hotelRevenueStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.HOTEL_REVENUE);
            Double hotelRevenue = hotelRevenueStr==null?null:Double.valueOf(hotelRevenueStr);
            Double hotelRevenueUSD = null;
            Double exchangeRate = currencyExchangeRates.get(currencyType);

            if (hotelRevenue!=null && exchangeRate!=null && exchangeRate!=0) {
                hotelRevenueUSD = hotelRevenue / exchangeRate;
            }
            String averageDailyRateStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.AVERAGE_DAILY_RATE);
            Double averageDailyRate = averageDailyRateStr==null?null:Double.valueOf(averageDailyRateStr);
            Double averageDailyRateUSD = null;
            if (averageDailyRate!=null && exchangeRate!=null && exchangeRate!=0) {
                averageDailyRateUSD = averageDailyRate / exchangeRate;
            }

            // if the request has the date format explicitly, we should just use it ; otherwise we will use the default date format (yyyy-MM-dd)
            String df = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.DATE_FORMAT);
            String dateFormat = (df!=null&&df.length()>0)?df:OpinmindConstants.DEFAULT_DATE_FORMAT;

            Date checkinDate = null;
            String checkinDateStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.CHECKIN_DATE);
            if (checkinDateStr != null){
                checkinDateStr = checkinDateStr.trim();

                try {
                    checkinDate = OpinmindUtil.stringToTimestamp(checkinDateStr, dateFormat);
                } catch (ParseException e) {
                    // do nothing
                }
            }

            Date checkoutDate = null;
            String checkoutDateStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.CHECKOUT_DATE);
            if (checkoutDateStr != null){
                checkoutDateStr = checkoutDateStr.trim();

                try {
                    checkoutDate = OpinmindUtil.stringToTimestamp(checkoutDateStr, dateFormat);
                } catch (ParseException e) {
                    // do nothing
                }
            }

            String numRoomsStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.NUM_ROOMS);
            Integer numRooms = numRoomsStr==null?null:Integer.valueOf(numRoomsStr);
            String numGuestsStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.NUM_GUESTS);
            Integer numGuests = numGuestsStr==null?null:Integer.valueOf(numGuestsStr);

            String propertyName = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.PROPERTY_NAME);
            String roomType = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.ROOM_TYPE);

            String hotelCode = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.HOTEL_CODE);
            String hotelBrand = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.HOTEL_BRAND);

            String destCity = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.DEST_CITY);
            String destState = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.DEST_STATE);
            String destCountry = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.DEST_COUNTRY);

            String hotelConfirmationNumber = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.HOTEL_CONFIRMATION_NUMBER);
            String hotelMembershipLevel = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.HOTEL_MEMBERSHIP_LEVEL);

            // flight fields
            String flightRevenueStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.FLIGHT_REVENUE);
            Double flightRevenue = flightRevenueStr==null?null:Double.valueOf(flightRevenueStr);
            Double flightRevenueUSD = null;
            if (flightRevenue!=null && exchangeRate!=null && exchangeRate!=0) {
                flightRevenueUSD = flightRevenue / exchangeRate;
            }

            String averageAirfareStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.AVERAGE_AIRFARE);
            Double averageAirfare = averageAirfareStr==null?null:Double.valueOf(averageAirfareStr);
            Double averageAirfareUSD = null;
            if (averageAirfare!=null && exchangeRate!=null && exchangeRate!=0) {
                averageAirfareUSD = averageAirfare / exchangeRate;
            }

            String numPassengersStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.NUM_PASSENGERS);
            Integer numPassengers = numPassengersStr==null?null:Integer.valueOf(numPassengersStr);

            Date departureDate = null;
            String departureDateStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.DEPARTURE_DATE);
            if (departureDateStr != null){
                departureDateStr = departureDateStr.trim();

                try {
                    departureDate = OpinmindUtil.stringToTimestamp(departureDateStr, dateFormat);
                } catch (ParseException e) {
                    // do nothing
                }

            }

            Date returnDate = null;
            String returnDateStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.RETURN_DATE);
            if (returnDateStr != null){
                returnDateStr = returnDateStr.trim();

                try {
                    returnDate = OpinmindUtil.stringToTimestamp(returnDateStr, dateFormat);
                } catch (ParseException e) {
                    // do nothing
                }
            }


            String flightNumber = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.FLIGHT_NUMBER);

            String destAirportCode = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.DEST_AIRPORT_CODE);
            String airline = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.AIRLINE);
            String originAirportCode = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.ORIGIN_AIRPORT_CODE);
            String cabinClass = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.CABIN_CLASS);

            String flightConfirmationNumber = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.FLIGHT_CONFIRMATION_NUMBER);
            String flightMembershipLevel = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.FLIGHT_MEMBERSHIP_LEVEL);

            // other fields
            String otherRevenueStr = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.OTHER_REVENUE);
            Double otherRevenue = otherRevenueStr==null?null:Double.valueOf(otherRevenueStr);
            Double otherRevenueUSD = null;
            if (otherRevenue!=null && exchangeRate!=null && exchangeRate!=0) {
                otherRevenueUSD = otherRevenue / exchangeRate;
            }
            // flex fields
            String flex1 = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.FLEX1);
            String flex2 = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.FLEX2);
            String flex3 = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.FLEX3);
            String flex4 = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.FLEX4);
            String flex5 = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.FLEX5);
            String flex6 = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.FLEX6);
            String flex7 = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.FLEX7);
            String flex8 = targetMap.get(paramPrefix+ConversionServerConstants.HTTP_PARAM.FLEX8);

            // create the conversion extension record, leave the id field empty for now
            ConversionExtension conversionExtension = null;
            if (id!=null && conversionPixelId!=null && cookieId!=null && conversionTs!=null &&
                    ( activityType!=null || currencyType!=null ||
                            hotelRevenue!=null || averageDailyRate!=null || checkinDate!=null || checkoutDate!=null ||
                            numRooms!=null || numGuests!=null || propertyName!=null || roomType!=null ||
                            hotelCode!=null || hotelBrand!=null || destCity!=null || destState!=null || destCountry!=null ||
                            hotelConfirmationNumber!=null || hotelMembershipLevel!=null ||
                            flightRevenue!=null || averageAirfare!=null || numPassengers!=null ||
                            departureDate!=null || returnDate!=null || flightNumber!=null ||
                            destAirportCode!=null || airline!=null || originAirportCode!=null || cabinClass!=null ||
                            flightConfirmationNumber!=null || flightMembershipLevel!=null || otherRevenue!=null ||
                            flex1!=null || flex2!=null || flex3!=null || flex4!=null || flex5!=null || flex6!=null || flex7!=null || flex8!=null ) ) {
                conversionExtension = new ConversionExtension(
                        Long.valueOf(id),
                        Integer.valueOf(conversionPixelId),
                        cookieId,
                        new Date(Long.valueOf(conversionTs)),
                        activityType,
                        currencyType,
                        hotelRevenue,
                        hotelRevenueUSD,
                        averageDailyRate,
                        averageDailyRateUSD,
                        checkinDate,
                        checkoutDate,
                        numRooms,
                        numGuests,
                        propertyName,
                        roomType,
                        hotelCode,
                        hotelBrand,
                        destCity,
                        destState,
                        destCountry,
                        hotelConfirmationNumber,
                        hotelMembershipLevel,
                        flightRevenue,
                        flightRevenueUSD,
                        averageAirfare,
                        averageAirfareUSD,
                        numPassengers,
                        departureDate,
                        returnDate,
                        flightNumber,
                        destAirportCode,
                        airline,
                        originAirportCode,
                        cabinClass,
                        flightConfirmationNumber,
                        flightMembershipLevel,
                        otherRevenue,
                        otherRevenueUSD,
                        flex1,
                        flex2,
                        flex3,
                        flex4,
                        flex5,
                        flex6,
                        flex7,
                        flex8);

                conversionExtensionLogger.log(conversionExtension);
            }

            if (exchangeRate==null || exchangeRate==0) {
                if (flightRevenue!=null || hotelRevenue!=null || otherRevenue!=null || averageAirfare!=null || averageDailyRate!=null)
                    log.warn("exchange rate not available for this conversion. currencyType: " + currencyType);
            }
        }
    }

    private void currencyExchangeRatesRefresh() {
        Map<String, Double> tmpCurrencyExchangeRates  = new HashMap<String, Double>();

        if (dataSource != null) {
            String query = "select currency_code, exchange_rate_from_usd from currencies";

            Connection connection = null;
            Statement s = null;
            ResultSet rs = null;
            try {
                connection = dataSource.getConnection();
                s = connection.createStatement();
                s.executeQuery(query);
                rs = s.getResultSet();

                while (rs.next()) {
                    String currencyCode = rs.getString("currency_code");
                    Double exchangeRate = rs.getDouble("exchange_rate_from_usd");

                    tmpCurrencyExchangeRates.put(currencyCode, exchangeRate);
                }

                currencyExchangeRates = tmpCurrencyExchangeRates;
            }
            catch (Exception e) {
                log.warn("refresh currency exchange failed", e);
            }
            finally {
                DBUtil.close(rs, s, connection);
            }
        }
        else {
            log.warn("can't refresh currency exchange rates");
        }
    }


    @Override
    public void validate(String[] data, String fileName, int lineNo)
            throws Exception {
        // for now, do nothing
    }

    public CentralLogger getCookieKeyValueLogger() {
        return cookieKeyValueLogger;
    }

    public void setCookieKeyValueLogger(CentralLogger cookieKeyValueLogger) {
        this.cookieKeyValueLogger = cookieKeyValueLogger;
    }

    public CentralLogger getEventKeyValueLogger() {
        return eventKeyValueLogger;
    }

    public void setEventKeyValueLogger(CentralLogger eventKeyValueLogger) {
        this.eventKeyValueLogger = eventKeyValueLogger;
    }

    /**
     * @return the conversionLogLogger
     */
    public CentralLogger getConversionLogLogger() {
        return conversionLogLogger;
    }

    /**
     * @param conversionLogLogger the conversionLogLogger to set
     */
    public void setConversionLogLogger(CentralLogger conversionLogLogger) {
        this.conversionLogLogger = conversionLogLogger;
    }

    /**
     * @return the conversionExtensionLogger
     */
    public CentralLogger getConversionExtensionLogger() {
        return conversionExtensionLogger;
    }

    /**
     * @param conversionExtensionLogger the conversionExtensionLogger to set
     */
    public void setConversionExtensionLogger(CentralLogger conversionExtensionLogger) {
        this.conversionExtensionLogger = conversionExtensionLogger;
    }

    /**
     * @return the dataSource
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * @param dataSource the dataSource to set
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}

