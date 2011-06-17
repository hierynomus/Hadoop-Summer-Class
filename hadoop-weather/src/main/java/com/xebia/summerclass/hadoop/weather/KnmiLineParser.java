package com.xebia.summerclass.hadoop.weather;

import org.apache.hadoop.io.Text;

public class KnmiLineParser {


    public static enum KnmiLineType {
        COMMENT, EMPTY, DATA, UNPARSABLE;
    }

    public static enum KnmiLineField {
        STN(0), YYYYMMDD(1), RH(22);

        public final int position;

        private KnmiLineField(int position) {
            this.position = position;
        }
    }

    private static int EXPECTED_FIELDS = 41;
    private static String COMMENT_PREFIX = "#";

    private String[] fields;

    public KnmiLineType parse(Text line) {
        return this.parse(line.toString());
    }

    public KnmiLineType parse(String line) {
        resetState();

        if (line.startsWith(COMMENT_PREFIX)) {
            return KnmiLineType.COMMENT;
        }

        if (line.isEmpty()) {
            return KnmiLineType.EMPTY;
        }

        this.fields = line.split(",\\s*");
        if (this.fields.length != EXPECTED_FIELDS) {
            return KnmiLineType.UNPARSABLE;
        }

        return KnmiLineType.DATA;
    }

    private void resetState() {
        this.fields = null;
    }

    public String getStation() {
        return this.fields[KnmiLineField.STN.position].trim();
    }

    public String getDate() {
        return this.fields[KnmiLineField.YYYYMMDD.position].trim();
    }

    public long getPrecipitation() {
        return Long.parseLong(this.fields[KnmiLineField.RH.position]);
    }
}
