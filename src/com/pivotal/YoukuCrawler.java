package com.pivotal;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.util.regex.Pattern;

/**
 * Created by James on 3/5/14.
 */
public class YoukuCrawler extends WebCrawler {

    private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g"
                + "|png|tiff?|mid|mp2|mp3|mp4"
                + "|wav|avi|mov|mpeg|ram|m4v|pdf"
                + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

        @Override
        public boolean shouldVisit(WebURL url) {
            String href = url.getURL().toLowerCase();
            return !FILTERS.matcher(href).matches() && href.indexOf("http://index.youku.com/") >= 0;
        }

        @Override
        public void visit(Page page) {
            String url = page.getWebURL().getURL();
            System.out.println("URL: " + url);

            if (page.getParseData() instanceof HtmlParseData) {
                HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
                String html = htmlParseData.getHtml();
                Document doc = Jsoup.parse(html);
                Elements e = doc.getElementsByAttributeValue("class", "analyse_num");
                Elements e1 = doc.getElementsByAttributeValue("class", "show_name");
                try{
                    if(e.size()>0 && e1.size()>0){
                        Connection conn = this.getConnection();
                        if(conn != null){
                            String sql = "INSERT INTO youku_movie_play(chinese_name, youku_play_num) " +
                                                       "VALUES (?, ?)";
                            PreparedStatement ps = conn.prepareStatement(sql);
                            ps.setString(1, e1.get(0).text());
                            ps.setDouble(2, DecimalFormat.getInstance().parse("".equalsIgnoreCase(e.get(0).getElementsByTag("span").text()) ? "0" : e.get(0).getElementsByTag("span").text()).doubleValue());
                            ps.executeUpdate();
                            if(ps != null){
                                ps.close();
                            }
                            conn.close();
                        }
                    }
                }catch(Exception a){
                    a.printStackTrace();
                }
            }
        }

    private Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mtime_movie?characterEncoding=utf-8", "root", "");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

}

