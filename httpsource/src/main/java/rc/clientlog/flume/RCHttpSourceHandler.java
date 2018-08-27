package rc.clientlog.flume;


import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RCHttpSourceHandler implements HTTPSourceHandler {

    private static final String HEADER_APP_KEY = "RC-App-Key";
    private static final String HEADER_USER_ID = "RC-User-ID";
    private static final String HEADER_SDK_VERSION = "RC-SDK-Version";
    private static final String HEADER_PLATFORM = "RC-Platform";
    private static final String HEADER_START_TIME = "RC-Start-Time";
    private static final String HEADER_END_TIME = "RC-End-Time";
    private static final String HEADER_USER_IP = "RC-User-IP";
    private static final String X_Real_IP = "X-Real-IP";

    private static Logger logger = Logger.getLogger(RCHttpSourceHandler.class);

    public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
        Map<String, String> headerMap = new HashMap<String, String>();
        headerMap.put(HEADER_APP_KEY, request.getHeader(HEADER_APP_KEY));
        headerMap.put(HEADER_USER_ID, request.getHeader(HEADER_USER_ID));
        headerMap.put(HEADER_SDK_VERSION, request.getHeader(HEADER_SDK_VERSION));
        headerMap.put(HEADER_PLATFORM, request.getHeader(HEADER_PLATFORM));
        // cloud be empty from 'X-Real-IP' (not proved), if so, will get remote ip from 'X-Forwarded-For'.
        headerMap.put(HEADER_USER_IP, request.getHeader(X_Real_IP));

        List<Event> eventList = new ArrayList<Event>();
        ServletFileUpload upload = new ServletFileUpload(new DiskFileItemFactory());
        List<FileItem> items = upload.parseRequest(request);
        for (FileItem fileItem : items) {
            String timeRange = RCUtils.getTimeRangeInGz(fileItem.get());
            int index = timeRange.indexOf("-");
            headerMap.put(HEADER_START_TIME, timeRange.substring(0, index));
            headerMap.put(HEADER_END_TIME, timeRange.substring(index + 1));
            checkHeader(headerMap);
            eventList.add(EventBuilder.withBody(fileItem.get(), headerMap));
        }
        return eventList;
    }

    public void configure(Context context) {
    }

    private void checkHeader(Map<String, String> headerMap) {
        if (headerMap.get(HEADER_USER_ID) == null) {
            String platform = headerMap.get(HEADER_PLATFORM);
            String sdkVer = headerMap.get(HEADER_SDK_VERSION);
            logger.error("userId = NULL, platform = " + platform + ", sdkVer = " + sdkVer);
        }
    }
}
