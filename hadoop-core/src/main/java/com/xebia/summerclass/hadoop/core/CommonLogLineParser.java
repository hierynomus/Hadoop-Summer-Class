package com.xebia.summerclass.hadoop.core;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

public class CommonLogLineParser {

    // http://en.wikipedia.org/wiki/Common_Log_Format
    private Pattern logLinePattern = Pattern.compile(
            "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (-|\\d+)");

    private DateFormat dateFormat;

    private String remoteHost;
    private Date date;
    private String request;
    private String requestMethod;
    private String requestUrl;
    private String requestProtocol;
    private int statusCode;
    private long contentLength;

    public CommonLogLineParser() {
        this.dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
    }

    public boolean parse(Text line) {
        return this.parse(line.toString());
    }

    public boolean parse(String line) {
        Matcher matcher = this.logLinePattern.matcher(line);

        resetState();
        try {
            if(matcher.matches()) {
                extractRemoteHost(matcher);
                extractDate(matcher);
                extractRequest(matcher);
                extractStatusCode(matcher);
                extractContentLength(matcher);
                return true;
            }
        } catch (ParseException e) {
        }
        return false;
    }

    private void resetState() {
        this.remoteHost = this.request = this.requestMethod = this.requestUrl = this.requestProtocol = null;
        this.date = null;
        this.statusCode = -1;
        this.contentLength = -1;
    }

    private void extractRemoteHost(Matcher matcher) {
        this.remoteHost = matcher.group(1);
    }

    private void extractDate(Matcher matcher) throws ParseException {
        this.date = this.dateFormat.parse(matcher.group(4));
    }

    private void extractRequest(Matcher matcher) {
        this.request = matcher.group(5);

        String[] requestElements = this.request.split(" ");
        this.requestMethod = requestElements[0];
        this.requestUrl = requestElements[1];
        this.requestProtocol = requestElements[2];
    }

    private void extractStatusCode(Matcher matcher) {
        this.statusCode = Integer.parseInt(matcher.group(6));
    }

    private void extractContentLength(Matcher matcher) {
        if ("-".equals(matcher.group(7))) {
            this.contentLength = 0;
        } else {
            this.contentLength = Long.parseLong(matcher.group(7));
        }
    }

    public String getRemoteHost() {
        return this.remoteHost;
    }

    public Date getDate() {
        return this.date;
    }

    public Object getRequest() {
        return this.request;
    }

    public String getRequestMethod() {
        return this.requestMethod;
    }

    public String getRequestUrl() {
        return this.requestUrl;
    }

    public String getRequestProtocol() {
        return this.requestProtocol;
    }

    public int getStatusCode() {
        return this.statusCode;
    }

    public long getContentLength() {
        return this.contentLength;
    }
}
