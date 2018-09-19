package rc.flume.interceptor;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RCLogHttpInter implements Interceptor {

    private static final String HEADER_APP_KEY = "AppKey";
    private static final String HEADER_USER_ID = "UserId";
    private static final String HEADER_SDK_VER = "SdkVer";
    private static final String HEADER_PLATFORM = "Platform";
    private static final String HEADER_USER_IP = "UserIp";
    private static final String HEADER_START_TIME = "Start";
    private static final String HEADER_END_TIME = "End";

    private static final String MINI_SDK_VER = "2.8.10";
    private static final String PLATFORM_ANDROID = "Android";
    private static final String PLATFORM_IOS = "iOS";
    private static final Logger logger = Logger.getLogger(RCLogHttpInter.class);

    private static String latestSdkVer;
    private RCHttpInterCounter httpInterCount;

    public void initialize() {
        httpInterCount = new RCHttpInterCounter("RCLogInterceptor");
        httpInterCount.start();
    }

    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String sdkVer = headers.get(HEADER_SDK_VER);
        String platform = headers.get(HEADER_PLATFORM);
        if (!platform.equals(PLATFORM_ANDROID) && !platform.equals(PLATFORM_IOS)) {
            httpInterCount.incrementPlatformNullCount();
        } else if (sdkVer.equals("")) {
            httpInterCount.incrementSdkVerNullCount();
        } else if (RCUtils.compareVer(sdkVer, MINI_SDK_VER) < 0) {
            httpInterCount.incrementSdkVerOlderCount();
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
                String[] timeRange = RCUtils.getTimeRangeInGz(event.getBody());
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

        private static final String LATEST_SDK_VER = "latestsdkver";

        public Interceptor build() {
            return new RCLogHttpInter();
        }

        public void configure(Context context) {
            latestSdkVer = context.getString(LATEST_SDK_VER);
        }
    }
}
