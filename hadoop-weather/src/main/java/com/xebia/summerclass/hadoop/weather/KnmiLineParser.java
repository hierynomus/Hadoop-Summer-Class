package com.xebia.summerclass.hadoop.weather;

import org.apache.hadoop.io.Text;

public class KnmiLineParser {


    public static enum KnmiLineType {
        COMMENT, DATA, EMPTY, UNPARSABLE;
    }

    private static enum KnmiLineField {
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

        if (line.isEmpty()) {
            return KnmiLineType.EMPTY;
        }

        if (line.startsWith(COMMENT_PREFIX)) {
            return KnmiLineType.COMMENT;
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

    private String getStringField(KnmiLineField field) {
        return this.fields[field.position].trim();
    }

    private long getLongField(KnmiLineField field) {
        return Long.parseLong(this.fields[field.position]);
    }

    public String getStation() {
        return getStringField(KnmiLineField.STN);
    }

    public String getDate() {
        return getStringField(KnmiLineField.YYYYMMDD);
    }

    public long getPrecipitation() {
        return getLongField(KnmiLineField.RH);
    }
}
