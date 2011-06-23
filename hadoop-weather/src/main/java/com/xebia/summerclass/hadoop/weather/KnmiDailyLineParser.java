package com.xebia.summerclass.hadoop.weather;


public class KnmiDailyLineParser extends KnmiLineParser {

    public KnmiDailyLineParser() {
        setExpectedFieldCount(41);
    }

    private static enum KnmiDailyLineField {
        STN(0), YYYYMMDD(1), RH(22);

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

    public long getPrecipitation() {
        return getLongField(KnmiDailyLineField.RH.position);
    }
}
