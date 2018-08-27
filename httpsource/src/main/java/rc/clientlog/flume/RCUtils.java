package rc.clientlog.flume;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

class RCUtils {

    private static final String UNKNOWN= "Unknown";
    private static Logger logger = Logger.getLogger(RCHttpSourceHandler.class);

    static String getTimeRangeInGz(byte[] gzByte) {
        ByteArrayInputStream in = new ByteArrayInputStream(gzByte);
        String start = UNKNOWN;
        String end = UNKNOWN;
        try {
            GZIPInputStream ungzip = new GZIPInputStream(in);
            InputStreamReader isReader = new InputStreamReader(ungzip);
            BufferedReader bReader = new BufferedReader(isReader);
            String readLine = bReader.readLine();
            logger.info("startline = " + readLine);
            start = parseTimestamp(readLine);
            String lastLine = null;
            while ((readLine = bReader.readLine()) != null) {
                lastLine = readLine;
            }
            logger.info("endline = " + lastLine);
            end = parseTimestamp(lastLine);
        } catch (IOException e) {
            logger.info("unzip failed", e);
            e.printStackTrace();
        }
        return start + "-" + end;
    }

    private static String parseTimestamp(String line) {
        if (line.isEmpty()) {
            return null;
        }
        String TIME_MARK = "\"time\":";
        String timeStr = "Unknown";
        int start = line.indexOf(TIME_MARK);
        if (start >= 0) {
            start = start + TIME_MARK.length();
            int end = line.indexOf(",", start);
            if (end >= 0) {
                timeStr = line.substring(start, end);
            }
        }

        if (isRegexMatch(timeStr, "\\d{13}")) {
            return timeStr.substring(0, 10);
        } else if (isRegexMatch(timeStr, "\"\\d{2}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}\"")) {
            timeStr = timeStr.substring(1, timeStr.length() - 1);
            return date2Timestamp(timeStr, "yy-MM-dd HH:mm:ss.SSS");
        } else if (isRegexMatch(timeStr, "\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}\"")) {
            timeStr = timeStr.substring(1, timeStr.length() - 1);
            return date2Timestamp(timeStr, "yyyy-MM-dd HH:mm:ss.SSS");
        } else if (isRegexMatch(timeStr, "\"\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}\"")) {
            timeStr = timeStr.substring(1, timeStr.length() - 1);
            timeStr = RCUtils.getLogYearStr(timeStr) + "-" + timeStr;
            return date2Timestamp(timeStr, "yy-MM-dd HH:mm:ss.SSS");
        } else {
            return timeStr;
        }
    }

    private static boolean isRegexMatch(String content, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(content);
        return matcher.find();
    }

    private static String date2Timestamp(String timeStr, String format) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            return String.valueOf(sdf.parse(timeStr).getTime() / 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return UNKNOWN;
    }

    public static void main(String[] args) {
        String line = "{\"time\":\"12-27 03:05:22.909\",\"pid\":6912,\"tag\":\"L-ping_pong-S\",\"timer\":150000}";
        String tm = RCUtils.parseTimestamp(line);
        System.out.println(tm);
    }

    private static String getLogYearStr(String timeStr) {
        Calendar calendar = Calendar.getInstance();
        int logYear = calendar.get(Calendar.YEAR);
        try {
            int logMonth = Integer.parseInt(timeStr.substring(0, 2));
            int nowMonth = calendar.get(Calendar.MONTH) + 1;
            if (logMonth == 12 && nowMonth != logMonth) {
                logYear = logYear - 1;
            }
        } catch (NumberFormatException e) {
            logger.error("getLogYearStr() exception! logMonth = " + timeStr.substring(0, 2));
        }
        return String.valueOf(logYear);
    }
}
