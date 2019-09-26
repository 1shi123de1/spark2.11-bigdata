package project.imooc.ylqdh.bigdata.utils;

import project.imooc.ylqdh.bigdata.domain.MyUserAgentInfo;

import java.io.*;

/*
数据样例：
113.77.139.245	-	-	[30/Jan/2019:00:00:24 +0800]	"GET /static/page/index/newcomer.js?v=201901251938 HTTP/1.1"	200	1327	"www.imooc.com"	"https://www.imooc.com/"	-	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3642.0 Safari/537.36"	"-"	10.100.136.64:80 200 0.001 0.001

要解析的目标如下：
中间两个 - - 去掉不要
113.77.139.245  ip解析出country、province、city三个字段，
                因为解析不出来country字段，所以用运营商 operator 代替
[30/Jan/2019:00:00:24 +0800] 中的时间原样拿下来 time
"GET /static/page/index/newcomer.js?v=201901251938 HTTP/1.1" 解析出method、url、protocal 三个字段
200     请求的状态 status
1327    发送的字节长度 bytesent
"https://www.imooc.com/"    从什么地方过来 referer
"Mozilla/5.0 (Windows NT 10.0.."    ua 解析出 browsername、browserversion、osname、osversion
 */
public class LogFormat {

    public String parse(String datum) {
        String[] data = datum.split("\t");
        String result = null;

        // ip 解析
        String ip;
        String operator;
        String city;
        String province;
        IPAddressUtils ipu = new IPAddressUtils();
        ipu.init();
        ip = data[0];
        province = ipu.getIPLocation(ip).getCountry();
        city = ipu.getCity(ip);
        operator = ipu.getIPLocation(ip).getArea();

        // 时间
        String time;
        time = data[3].substring(1, data[3].length() - 1);

        // post 解析
        String method;
        String url;
        String protocal;
        String[] tmp = data[4].split(" ");
        method = tmp[0].substring(1, tmp[0].length());
        url = tmp[1];
        protocal = tmp[2].substring(0, tmp[2].length() - 1);

        // status
        String status;
        status = data[5];

        // bytesent
        String bytesent;
        bytesent = data[6];

        // referer
        String referer;
        referer = data[8].substring(1, data[8].length() - 1);

        // 解析Useragent
        String browserName;
        String browserVersion;
        String osName;
        String osVersion;
        MyUserAgentInfo info = UAUtils.getUserAgetnInfo(data[10]);
        browserName = info.getBrowserName();
        browserVersion = info.getBrowserVersion();
        osName = info.getOsName();
        osVersion = info.getOsVersion();

        result = ip + "\t" + province + "\t" + city + "\t" + operator
                + "\t" + time + "\t" + url + "\t" + method + "\t" + protocal + "\t"
                + status + "\t" + bytesent + "\t" + referer
                + "\t" + browserName + "\t" + browserVersion
                + "\t" + osName + "\t" + osVersion;

        return result;
    }

    public void format(String infile,String outfile) {

        try (
                // 读文件流
                FileReader reader = new FileReader(infile);
                BufferedReader bufferedReader = new BufferedReader(reader);
        )
        {
            // 打开写文件流
            File out = new File(outfile);
            out.createNewFile();    // 创建新文件，有同名文件则覆盖
            FileWriter writer = new FileWriter(out);
            BufferedWriter bufferedWriter = new BufferedWriter(writer);

            String line;
            String parseLine;
            while ((line = bufferedReader.readLine()) != null) {
                parseLine = parse(line);  // 读取一行，解析一行
                bufferedWriter.write(parseLine+"\r\n");   // 解析后的结果写入文件中,不要忘记换行
            }

            // 关闭文件输入输出流
            bufferedWriter.flush();
            bufferedWriter.close();
            bufferedReader.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

}
