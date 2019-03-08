package io.github.ppolushkin.newsAnalizer;

import io.github.ppolushkin.newsAnalizer.newsclient.ApiClient;
import io.github.ppolushkin.newsAnalizer.newsclient.Article;
import io.github.ppolushkin.newsAnalizer.newsclient.News;
import io.github.ppolushkin.newsAnalizer.output.CsvFileWriter;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


@Log
@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final String OTHER_TOPIC = "OTHER";

    @Value("${date}")
    private String date;

    @Value("${topics}")
    private String topics;

    @Value("${retriesOnError}")
    private Integer retriesOnError;

    @Autowired
    private ApiClient apiClient;

    @Autowired
    private CsvFileWriter csvFileWriter;

    @PostConstruct
    private void init() {
        log.info("***********************************");
        log.info("Date:" + date);
        log.info("topics: " + topics);
        log.info("***********************************");
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Application is running!");

        Map<String, AtomicInteger> topicsCount = new HashMap<>();
        for (String topic : topics.trim().split("\\s*,\\s*")) {
            topicsCount.put(topic, new AtomicInteger(0));
        }

        for (int dayInterval = 0; dayInterval < 10; dayInterval++) {

            News news;
            int attempt = 1;
            do {
                log.info("Read news for " + date + " [" + dayInterval + "]");
                news = apiClient.readNews(date, dayInterval);
                if (news != null) {
                    printNews(dayInterval, news);
                    analyzeNews(topicsCount, news);
                } else {
                    log.info("Error occurred on attempt " + attempt);
                }
                attempt++;
            } while (news == null && attempt < retriesOnError);
        }

        log.info("******************************");
        log.info("RESULTS");
        log.info("******************************");
        for (String topic: topicsCount.keySet()) {
            log.info(topic + ": " + topicsCount.get(topic).get());
        }

        csvFileWriter.write(topics.replace(",",":") + "_" + date + ".csv", topics, topicsCount);
    }

    private void printNews(int dayInterval, News news) {
        log.info("Found " + news.getTotalResults() + " news for " + date + " [" + dayInterval + "]");
        for (Article a : news.getArticles()) {
            log.info(a.getTitle());
            log.info(a.getDescription());
            log.info("-----------------------");
        }
    }

    private void analyzeNews(Map<String, AtomicInteger> topicsCount, News news) {
        for (Article article: news.getArticles()) {
            boolean foundTopic = false;
            for (String topic: topicsCount.keySet()) {
                if (hasArticleTopic(article, topic)) {
                    topicsCount.get(topic).incrementAndGet();
                    foundTopic = true;
                }
            }

            if (!foundTopic && topicsCount.containsKey(OTHER_TOPIC)) {
                topicsCount.get(OTHER_TOPIC).incrementAndGet();
            }
        }
    }

    private boolean hasArticleTopic(Article article, String topic) {
        String title = article.getTitle().toUpperCase(Locale.ENGLISH);
        String description = article.getDescription().toUpperCase(Locale.ENGLISH);

        return title.contains(topic)||
                title.contains(topic.toUpperCase()) ||
                description.contains(topic) ||
                description.contains(topic.toUpperCase());
    }

}
