package com.quicklybly.bigdata.spark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;

public class LogGenerator {

    private static final long SEED = 42;
    private static final int IP_COUNT = 5_000;
    private static final double BROKEN_LINE_SHARE = 0.01;

    private static final String[] METHODS = {"GET", "GET", "GET", "GET", "POST", "PUT", "DELETE"};
    private static final String[] PATHS = {
            "/", "/index.html", "/login", "/logout", "/api/items", "/api/items/42", "/api/orders",
            "/api/users/me", "/static/app.js", "/static/style.css", "/images/logo.png", "/search?q=spark",
    };
    private static final int[] STATUSES = {200, 200, 200, 200, 200, 200, 301, 304, 400, 401, 403, 404, 404, 500, 503};
    private static final String[] AGENTS = {
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0",
            "curl/8.4.0",
            "python-requests/2.32.3",
            "Googlebot/2.1 (+http://www.google.com/bot.html)",
    };
    private static final String[] BROKEN = {
            "",
            "-",
            "garbage line without any structure",
            "999.1.1 - - [bad date] \"GET\"",
    };

    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: LogGenerator <output file> <line count>");
            System.exit(1);
        }
        Path out = Path.of(args[0]);
        long lines = Long.parseLong(args[1]);

        Random random = new Random(SEED);
        String[] ips = new String[IP_COUNT];
        for (int i = 0; i < IP_COUNT; i++) {
            ips[i] = (10 + random.nextInt(200)) + "." + random.nextInt(256) + "."
                    + random.nextInt(256) + "." + (1 + random.nextInt(254));
        }

        ZonedDateTime time = ZonedDateTime.of(2026, 10, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        Files.createDirectories(out.toAbsolutePath().getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(out)) {
            for (long i = 0; i < lines; i++) {
                time = time.plusNanos(random.nextInt(200_000_000));
                if (random.nextDouble() < BROKEN_LINE_SHARE) {
                    writer.write(pick(random, BROKEN));
                } else {
                    writer.write(line(random, ips, time));
                }
                writer.newLine();
            }
        }
        System.out.printf("Wrote %,d lines to %s (%,d bytes)%n", lines, out.toAbsolutePath(), Files.size(out));
    }

    private static String line(Random random, String[] ips, ZonedDateTime time) {
        String method = pick(random, METHODS);
        int status = STATUSES[random.nextInt(STATUSES.length)];
        String bytes = status == 304 ? "-" : String.valueOf(200 + random.nextInt(50_000));
        return skewedIp(random, ips) + " - - [" + TIME.format(time) + "] \""
                + method + " " + pick(random, PATHS) + " HTTP/1.1\" " + status + " " + bytes
                + " \"-\" \"" + pick(random, AGENTS) + "\"";
    }

    private static String skewedIp(Random random, String[] ips) {
        double u = random.nextDouble();
        return ips[(int) (u * u * u * ips.length)];
    }

    private static String pick(Random random, String[] values) {
        return values[random.nextInt(values.length)];
    }
}
