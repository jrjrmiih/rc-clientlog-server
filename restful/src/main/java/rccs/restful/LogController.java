package rccs.restful;

import org.springframework.context.annotation.Role;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class LogController {

//    private HbaseConnection conn = HbaseConnection.getInstance();

//    @RequestMapping(value = "/sum", method = RequestMethod.GET)
//    public ResponseEntity<String> getSum() {
//        return null;
//    }
//
//    @RequestMapping(value = "/sum", method = RequestMethod.DELETE)
//    public ResponseEntity<String> delSum() {
//        int sum = conn.delSum();
//        return new ResponseEntity<>(String.valueOf(sum), sum >= 0 ? HttpStatus.OK : HttpStatus.NOT_ACCEPTABLE);
//    }

    @RequestMapping(value = "/log/{appKey}", method = RequestMethod.GET)
    public ResponseEntity<String> queryLogForApp(@PathVariable("appKey") String appKey) {
        String result = null;
        return new ResponseEntity<>(result, HttpStatus.OK);
    }


    @RequestMapping(value = "/log/{appKey}/{userId}", method = RequestMethod.GET)
    public ResponseEntity<String> queryLogForUser(@PathVariable("appKey") String appKey, @PathVariable("userId") String userId) {
//        String result;
//        try {
//            result = conn.getLog(appKey, userId, null, null);
//        } catch (Exception e) {
//            result = "Runtime exception!";
//            e.printStackTrace();
//        }
//        result = result != null ? result : "Log not found!";
        String result = "sdfasf";
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

//    @RequestMapping(value = "/log/{appKey}/{userId}/{range}", method = RequestMethod.GET)
//    public ResponseEntity<String> queryLogForRange(@PathVariable("appKey") String appKey,
//                                                   @PathVariable("userId") String userId,
//                                                   @PathVariable("range") String range) {
//        String result;
//        try {
//            String[] times = range.split("-");
//            result = conn.getLog(appKey, userId, times[0], times[1]);
//        } catch (IOException e) {
//            result = "IO exception!";
//            e.printStackTrace();
//        }
//        result = result != null ? result : "Log not found!";
//        return new ResponseEntity<>(result, HttpStatus.OK);
//    }
}