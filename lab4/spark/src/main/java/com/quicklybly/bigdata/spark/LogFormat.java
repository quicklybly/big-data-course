package com.quicklybly.bigdata.spark;

import java.util.regex.Pattern;

public final class LogFormat {

    public static final String IP_REGEX = "^(\\d{1,3}(?:\\.\\d{1,3}){3}) ";

    public static final Pattern IP_PATTERN = Pattern.compile(IP_REGEX);

    private LogFormat() {
    }
}
