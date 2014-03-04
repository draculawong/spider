package com.pivotal;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by James on 2/17/14.
 */
public class MtimeCrawler extends WebCrawler {

    private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g"
            + "|png|tiff?|mid|mp2|mp3|mp4"
            + "|wav|avi|mov|mpeg|ram|m4v|pdf"
            + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

    @Override
    public boolean shouldVisit(WebURL url) {
        String href = url.getURL().toLowerCase();
        return !FILTERS.matcher(href).matches() && href.indexOf("http://movie.mtime.com/") >= 0;
    }

    @Override
    public void visit(Page page) {
        String url = page.getWebURL().getURL();
        System.out.println("URL: " + url);

        if (page.getParseData() instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            String html = htmlParseData.getHtml();
            Document doc = Jsoup.parse(html);
            Connection conn = this.getConnection();
            if(conn != null){
                try {
                    String sql = "INSERT INTO movie(chinese_name, english_name, director, starring, type, " +
                            "release_date, rate, votes, region, runtime, certification, language, company) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setString(1, doc.select("span[property=v:itemreviewed]").text());
                    ps.setString(2, doc.select("span[class=ml9 px24]").text());
                    ps.setString(3, doc.select("a[rel=v:directedBy]").text());
                    ps.setString(4, doc.select("a[rel=v:starring]").text());
                    ps.setString(5, doc.select("a[property=v:genre]").text());
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    ps.setDate(6, doc.select("span[property=v:initialReleaseDate]").attr("content") == null || "".equalsIgnoreCase(doc.select("span[property=v:initialReleaseDate]").attr("content")) ? null :
                            new java.sql.Date(sdf.parse(doc.select("span[property=v:initialReleaseDate]").attr("content")).getTime()));
                    ps.setFloat(7, doc.select("span[property=v:average]").text() == null || "".equalsIgnoreCase(doc.select("span[property=v:average]").text()) ? 0f : Float.parseFloat(doc.select("span[property=v:average]").text()));
                    ps.setDouble(8, doc.select("span[property=v:votes]").text() == null || "".equalsIgnoreCase(doc.select("span[property=v:votes]").text()) ? 0d : Double.parseDouble(doc.select("span[property=v:votes]").text()));
                    ps.setString(9, doc.getElementsByAttributeValueStarting("href", "http://movie.mtime.com/movie/search/section/?nation=").text());
                    String runtime = doc.select("span[property=v:runtime]").text();
                    if(runtime != null && !"".equalsIgnoreCase(runtime)){
                        String[] t = runtime.split("/");
                        if(t.length > 0){
                            Pattern pattern = Pattern.compile("\\d+");
                            Matcher matcher = pattern.matcher(t[0]);
                            while (matcher.find()) {
                                runtime = matcher.group(0);
                                break;
                            }
                        }
                    }

                    ps.setInt(10, runtime == null || "".equalsIgnoreCase(runtime) ? 0 : Integer.parseInt(runtime));
                    ps.setString(11, doc.getElementsByAttributeValueStarting("href", "http://movie.mtime.com/movie/search/section/?certification=").text());
                    ps.setString(12, doc.getElementsByAttributeValueStarting("href", "http://movie.mtime.com/movie/search/section/?language=").text());
                    ps.setString(13, doc.getElementsByAttributeValueStarting("href", "http://movie.mtime.com/company/").text());
                    ps.executeUpdate();
                    if(ps != null){
                        ps.close();
                    }
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
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