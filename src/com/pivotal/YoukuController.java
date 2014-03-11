package com.pivotal;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import org.apache.commons.codec.binary.Base64;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by James on 3/5/14.
 */
public class YoukuController {

    private static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mtime_movie?characterEncoding=utf-8", "root", "");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static String base64UrlEncode(String input) throws Exception {
        String result = null;
        Base64 encoder = new Base64(true);
        byte[] encodedBytes = encoder.encode(input.getBytes("UTF-8"));
        result = new String(encodedBytes);
        return result.replaceAll("(?:\\r\\n|\\n\\r|\\n|\\r)", "");
    }

    public static void main(String[] args) throws Exception {
            String crawlStorageFolder = "/Users/bjcoe/mtime_movie_data";
            int numberOfCrawlers = 100;
            int maxDepthOfCrawling = 0;

            CrawlConfig config = new CrawlConfig();
            config.setCrawlStorageFolder(crawlStorageFolder);
            config.setMaxDepthOfCrawling(maxDepthOfCrawling);

            PageFetcher pageFetcher = new PageFetcher(config);
            RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
            RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
            CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

            Connection conn = getConnection();
            if(conn != null){
                String sql = "SELECT chinese_name FROM movie";
                PreparedStatement ps = conn.prepareStatement(sql);
                ResultSet rs = ps.executeQuery();
                while(rs.next()){
                    controller.addSeed("http://index.youku.com/vr_keyword/id_" +
                            base64UrlEncode(rs.getString(1)) + "?type=alldata");
                }
                if(rs != null){
                    rs.close();
                }
                if(ps != null){
                    ps.close();
                }
                conn.close();
            }

            controller.startNonBlocking(YoukuCrawler.class, numberOfCrawlers);
            controller.waitUntilFinish();
        }
}
