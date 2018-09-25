package rc.flume.interceptor;

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

    private static Logger logger = Logger.getLogger(RCUtils.class);

    static int compareVer(String sdkVer, String minVer) {
        String[] sdkVers = sdkVer.split("\\.");
        String[] minVers = minVer.split("\\.");
        int idx = 0;
        int minLength = Math.min(sdkVers.length, minVers.length);
        int diff = 0;
        while (idx < minLength
                && (diff = sdkVers[idx].length() - minVers[idx].length()) == 0
                && (diff = sdkVers[idx].compareTo(minVers[idx])) == 0) {
            idx++;
        }
        diff = (diff != 0) ? diff : sdkVers.length - minVers.length;
        return diff;
    }

    static String[] getTimeRangeInGz(byte[] gzByte) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(gzByte);
        GZIPInputStream ungzip = new GZIPInputStream(in);
        InputStreamReader isReader = new InputStreamReader(ungzip);
        BufferedReader bReader = new BufferedReader(isReader);
        String readLine = bReader.readLine();
        String start = parseTimestamp(readLine);
        String lastLine = null;
        while ((readLine = bReader.readLine()) != null) {
            lastLine = readLine;
        }
        String end = parseTimestamp(lastLine);
        return new String[]{start, end};
    }

    private static String parseTimestamp(String line) {
        if (line == null || line.length() == 0) {
            return null;
        }
        String TIME_MARK = "\"time\":";
        String timeStr = null;
        int start = line.indexOf(TIME_MARK);
        if (start >= 0) {
            start = start + TIME_MARK.length();
            int end = line.indexOf(",", start);
            if (end >= start) {
                timeStr = line.substring(start, end);
            }
        }
        if (timeStr == null || timeStr.length() == 0) {
            return null;
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
            timeStr = getLogYearStr(timeStr) + "-" + timeStr;
            return date2Timestamp(timeStr, "yy-MM-dd HH:mm:ss.SSS");
        }
        return null;
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
        return null;
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

//    public static void main(String[] args) {
//    }
}
