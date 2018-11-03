package rccs.flume.source;


import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RccsHttpSrc implements HTTPSourceHandler {

    private static final String RC_APP_KEY = "RC-App-Key";
    private static final String RC_USER_ID = "RC-User-ID";
    private static final String RC_SDK_VERSION = "RC-SDK-Version";
    private static final String RC_PLATFORM = "RC-Platform";
//    private static final String RC_START_TIME = "RC-Start-Time";
//    private static final String RC_END_TIME = "RC-End-Time";
    private static final String X_FORWARDED_FOR = "X-Forwarded-For";

    private static final String HEADER_APP_KEY = "aid";
    private static final String HEADER_USER_ID = "uid";
    private static final String HEADER_SDK_VER = "ver";
    private static final String HEADER_PLATFORM = "os";
    private static final String HEADER_USER_IP = "uip";

    private static final int MAX_CONTENT_LEN = 1024 * 1024;

    private static final Logger logger = Logger.getLogger(RccsHttpSrc.class);

    public void configure(Context context) {
    }

    public List<Event> getEvents(HttpServletRequest request) {
        Map<String, String> headerMap = new HashMap<String, String>();
        String userIp = request.getHeader(X_FORWARDED_FOR);
        int pos = userIp.indexOf(",");
        if (pos > 0) {
            userIp = userIp.substring(0, pos);
        }
        String appKey = request.getHeader(RC_APP_KEY);
        String userId = request.getHeader(RC_USER_ID);
        String sdkVer = request.getHeader(RC_SDK_VERSION);
        String platform = request.getHeader(RC_PLATFORM);

        headerMap.put(HEADER_USER_IP, userIp);
        headerMap.put(HEADER_APP_KEY, appKey);
        headerMap.put(HEADER_USER_ID, userId == null ? "" : userId);
        headerMap.put(HEADER_SDK_VER, sdkVer == null ? "" : sdkVer);
        headerMap.put(HEADER_PLATFORM, platform == null ? "" : platform);

        List<Event> eventList = new ArrayList<Event>();
        if (request.getMethod().equals("POST")) {
            int conLen = request.getContentLength();
            if (conLen < MAX_CONTENT_LEN) {
                ServletFileUpload upload = new ServletFileUpload(new DiskFileItemFactory());
                try {
                    List<FileItem> items = upload.parseRequest(request);
                    for (FileItem fileItem : items) {
                        eventList.add(EventBuilder.withBody(fileItem.get(), headerMap));
                    }
                } catch (FileUploadException ignored) {
                }
            } else {
                logger.warn("RcLog: content over length - " + conLen);
            }
        }
        return eventList;
    }
}
