package project.weblog.ylqdh.bigdata.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Date;

/*
    模拟日志数据
    把需要的数据用数组形式存放，然后通过随机数，组合成一条日志数据
    一条日志数据格式如下：
    时间  uri  IP   referer   status_code
 */
public class LogGenerator {

    public static void main(String[] args) throws IOException {
        if(args.length != 1) {
            System.out.println("请输入日志保存的文件...");
            System.exit(1);
        }

        // 使用SecureRandom 真随机数
        SecureRandom random = new SecureRandom();

        // 格式化时间
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


        int[] ips = {45,168,198,23,48,10,89,68,147,132,112,59,65};
        String[] uris = {"/class/145.html","/class/146.html","/class/112.html",
                "/class/130.html","/class/141.html","/class/131.html","/course/list",
                "/learn/821"};
        String[] referers = {"https://www.baidu.com/s?wd=","https://www.sogou.com/web?query=",
        "https://cn.bing.com/search?q=","https://search.yahoo.com/search?p=","-","https://www.so.com/s?q="};
        String[] keywords = {"Spark SQL","Spark Streaming实战","spark","Hadoop基础",
                "Hive ETL","Storm实战","Flume+Kafka","大数据面试"};
        int[] status_codes = {200,205,300,304,400,404,500,504,599};

        // 打开写文件流,注意要追加写入
        File f = new File(args[0]);
        f.createNewFile();
        try(FileWriter writer = new FileWriter(f,true);
            BufferedWriter out = new BufferedWriter(writer)
        ){
            int i = 10;
            while (i>=1) {
                String ip = ips[random.nextInt(ips.length)]+"."+ips[random.nextInt(ips.length)]+
                        "."+ips[random.nextInt(ips.length)]+"."+ips[random.nextInt(ips.length)];
                String uri = uris[random.nextInt(uris.length)];
                String referer = referers[random.nextInt(referers.length)];
                String keyword = keywords[random.nextInt(keywords.length)];
                String statu_code = status_codes[random.nextInt(status_codes.length)]+"";

                String url;
                if("-".equals(referer)){
                    url = "-";
                }else {
                    url = referer + keyword;
                }

                String today = df.format(new Date());

                out.write(ip+"\t"+today+"\t\"GET "+uri+" HTTP/1.1\"\t"+statu_code+"\t"+url+"\r\n");
                out.flush();
//                System.out.println(ip+"\t"+today+"\t\"GET "+uri+" HTTP/1.1\"\t"+statu_code+"\t"+url);
                i--;
            }
        } catch (IOException e) {
                e.printStackTrace();
        }
    }
}