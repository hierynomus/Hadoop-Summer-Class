package com.xebia.summerclass.hadoop.weather;

import org.apache.hadoop.io.Text;

public class KnmiLineParser {

    public static enum KnmiLineType {
        COMMENT, DATA, EMPTY, UNPARSABLE;
    }

    private static String COMMENT_PREFIX = "#";

    private int expectedFieldCount;
    protected String[] fields;

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

        this.fields = line.split(",");
        if (this.fields.length != this.expectedFieldCount) {
            System.err.println(this.fields.length);
            return KnmiLineType.UNPARSABLE;
        }

        return KnmiLineType.DATA;
    }

    private void resetState() {
        this.fields = null;
    }

    protected void setExpectedFieldCount(int fields) {
        this.expectedFieldCount = fields;
    }

    protected String getStringField(int position) {
        return this.fields[position].trim();
    }

    protected long getLongField(int position) {
        return Long.parseLong(this.fields[position].trim());
    }
}
