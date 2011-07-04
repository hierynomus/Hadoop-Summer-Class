package com.xebia.summerclass.hadoop.weather;



public class KnmiHourlyLineParser extends KnmiLineParser {

    public KnmiHourlyLineParser() {
        setExpectedFieldCount(24);
    }

    private static enum KnmiDailyLineField {
        STN(0), YYYYMMDD(1), HH(2), T(7), RH(13);

        public final int position;

        private KnmiDailyLineField(int position) {
            this.position = position;
        }
    }

    public String getStation() {
        return getStringField(KnmiDailyLineField.STN.position);
    }

    public String getDate() {
        return getStringField(KnmiDailyLineField.YYYYMMDD.position);
    }

    public String getHour() {
        return getStringField(KnmiDailyLineField.HH.position);
    }

    public long getPrecipitation() {
        return getLongField(KnmiDailyLineField.RH.position);
    }

    public long getTemperature() {
        return getLongField(KnmiDailyLineField.T.position);
    }
}
