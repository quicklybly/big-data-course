package com.quicklybly.bigdata.spark;

import java.util.regex.Pattern;

public final class LogFormat {

    public static final String IP_REGEX = "^(\\d{1,3}(?:\\.\\d{1,3}){3}) ";

    public static final Pattern IP_PATTERN = Pattern.compile(IP_REGEX);

    public static final String LINE_REGEX =
            "^(\\d{1,3}(?:\\.\\d{1,3}){3}) \\S+ \\S+ \\[[^\\]]+\\] \"(\\S+) (\\S+) [^\"]*\" (\\d{3}) (\\S+)";

    public static final Pattern LINE_PATTERN = Pattern.compile(LINE_REGEX);

    private LogFormat() {
    }
}
