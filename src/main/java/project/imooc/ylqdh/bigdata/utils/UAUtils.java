package project.imooc.ylqdh.bigdata.utils;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang3.StringUtils;
import project.imooc.ylqdh.bigdata.domain.MyUserAgentInfo;

import java.io.IOException;

public class UAUtils {

    public static UASparser parser = null;

    static {
        try {
            parser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
        将官方解析出来的UserAgentInfo转换为自己需要的UserAgentInfo
        其实就是需要的字段做了转换而已
     */
    public static MyUserAgentInfo getUserAgetnInfo(String ua){
        MyUserAgentInfo info = null;

        try {
            // 判断参数ua是否为空
            if (StringUtils.isNoneEmpty(ua)){
                UserAgentInfo tmp = parser.parse(ua);
                if (null != tmp){
                    info = new MyUserAgentInfo();
                    info.setBrowserName(tmp.getUaFamily());
                    info.setBrowserVersion(tmp.getBrowserVersionInfo());
                    info.setOsName(tmp.getOsFamily());
                    info.setOsVersion(tmp.getOsName());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return info;
    }

    public static void main(String[] args) throws Exception {
//        UserAgentInfo info = parser.parse("Mozilla/4.0 (compatible; MSIE 7.0;Windows NT 5.1; )");
//
//        System.out.println(info);

        MyUserAgentInfo info = UAUtils.getUserAgetnInfo("Mozilla/4.0 (compatible; MSIE 7.0;Windows NT 5.1; )");

        System.out.println(info);
    }
}
