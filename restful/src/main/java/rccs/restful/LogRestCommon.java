package rccs.restful;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class LogRestCommon {

    /**
     * 获取指定格式的 UTC 10位时间戳。
     *
     * @param format 日期格式。
     * @param source 日期字符串。
     */
    static long parseTimestamp(String format, String source) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.parse(source).getTime() / 1000;
    }

    public static void main(String[] args) {
        HbaseConnection conn = HbaseConnection.getInstance();
    }
}
