
import org.apache.log4j.Logger;
import util.AreaUtils;
import util.DateUtils;
import util.PageUtils;

import java.util.Date;

public class LoggerGenerate {

    private static Logger logger = Logger.getLogger(LoggerGenerate.class.getName());

    public static void main(String[] args) throws InterruptedException {
        int value = 1;
        while (true) {
            Thread.sleep(1000);
            logger.info("value:" + value);
            value += 1;
            //logger.info(value);
        }
    }

    /**
     * 用户访问日志产生器
     */
//    public static String log(){
//
//        StringBuilder builder = new StringBuilder("用户访问,");
//
//        builder.append(DateUtils.getTime(new Date())).append(",")
//            .append(AreaUtils.getCityName(AreaUtils.getRandomIp())).append(",")
//            .append(PageUtils.getCourse(PageUtils.getRandomPage()));
//
//        return builder.toString();
//    }
}
