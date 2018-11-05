package rccs.flume.source;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RccsHttpItcptor implements Interceptor {

//    private static final String HEADER_APP_KEY = "aid";
    private static final String HEADER_USER_ID = "uid";
    private static final String HEADER_SDK_VER = "ver";
    private static final String HEADER_PLATFORM = "os";
//    private static final String HEADER_USER_IP = "uip";
    private static final String HEADER_START_TIME = "st";
    private static final String HEADER_END_TIME = "et";

    private static final String PLATFORM_ANDROID = "Android";
    private static final String PLATFORM_IOS = "iOS";

    private static String miniSdkVer;
    private static String latestSdkVer;
    private RccsHttpInterCounter httpInterCount;

    public void initialize() {
        httpInterCount = new RccsHttpInterCounter("RCLogInterceptor");
        httpInterCount.start();
    }

    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String sdkVer = headers.get(HEADER_SDK_VER);
        String platform = headers.get(HEADER_PLATFORM);
        if (RccsHttpInterUtils.compareVer(sdkVer, miniSdkVer) < 0) {
            httpInterCount.incrementSdkVerOlderCount();
        } else if (!platform.equals(PLATFORM_ANDROID) && !platform.equals(PLATFORM_IOS)) {
            httpInterCount.incrementPlatformNullCount();
        } else if (sdkVer.equals("")) {
            httpInterCount.incrementSdkVerNullCount();
        } else if (headers.get(HEADER_USER_ID).equals("")) {
            if (sdkVer.equals(latestSdkVer)) {
                if (platform.equals(PLATFORM_ANDROID)) {
                    httpInterCount.incrementLatestAndroidTimeFailedCount();
                } else {
                    httpInterCount.incrementLatestIosTimeFailedCount();
                }
            }
            httpInterCount.incrementUserIdNullCount();
        } else {
            try {
                String[] timeRange = RccsHttpInterUtils.getTimeRangeInGz(event.getBody());
                if (timeRange[0] == null || timeRange[1] == null) {
                    if (sdkVer.equals(latestSdkVer)) {
                        if (platform.equals(PLATFORM_ANDROID)) {
                            httpInterCount.incrementLatestAndroidTimeFailedCount();
                        } else {
                            httpInterCount.incrementLatestIosTimeFailedCount();
                        }
                    }
                    httpInterCount.incrementTimeFailedCount();
                } else {
                    headers.put(HEADER_START_TIME, timeRange[0]);
                    headers.put(HEADER_END_TIME, timeRange[1]);
                    event.setHeaders(headers);
                    return event;
                }
            } catch (IOException e) {
                if (sdkVer.equals(latestSdkVer)) {
                    if (platform.equals(PLATFORM_ANDROID)) {
                        httpInterCount.incrementLatestAndroidUnzipFailedCount();
                    } else {
                        httpInterCount.incrementLatestIosUnzipFailedCount();
                    }
                }
                httpInterCount.incrementUnzipFailedCount();
            }
        }
        return null;
    }

    public List<Event> intercept(List<Event> events) {
        List<Event> doneEvents = Lists.newArrayListWithCapacity(events.size());
        for (Event event : events) {
            Event doneEvent = intercept(event);
            if (doneEvent != null) {
                doneEvents.add(doneEvent);
            }
        }
        return doneEvents;
    }

    public void close() {
        httpInterCount.stop();
    }

    public static class Builder implements Interceptor.Builder {

        private static final String MINI_SDK_VER = "minisdkver";
        private static final String LATEST_SDK_VER = "latestsdkver";

        public Interceptor build() {
            return new RccsHttpItcptor();
        }

        public void configure(Context context) {
            miniSdkVer = context.getString(MINI_SDK_VER);
            latestSdkVer = context.getString(LATEST_SDK_VER);
        }
    }
}
