package com.pivotal;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

/**
 * Created by James on 2/17/14.
 */
public class MtimeController {
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

        for(int i = 10000; i<205340; i++){
            controller.addSeed("http://movie.mtime.com/"+i+"/");
        }
        controller.startNonBlocking(MtimeCrawler.class, numberOfCrawlers);
        controller.waitUntilFinish();
    }
}
