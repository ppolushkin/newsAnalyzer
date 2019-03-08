package io.github.ppolushkin.newsAnalizer.newsclient;

import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;

@Log
@Component
public class ApiClient {
//&from=2019-03-04&to=2019-03-04

    @Value("${newsApiUrl}")
    private String url;

    private final RestTemplate restTemplate = new RestTemplate();

    @PostConstruct
    private void init() {
        log.info("URL :" + url);
    }

    public News readNews(String date, Integer interval) {
        String url = this.url + params(date, interval);

        log.info("URL == " + url);

        try {
            ResponseEntity<News> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<News>() {
                    });
            return response.getBody();
        } catch (Exception e) {
            log.info("Exception occurred " + e);
            return null;
        }
    }

    //from=2019-03-04T23:00:00&to=2019-03-04T23:59:00
    private String params(String date, int interval) {

        switch (interval) {
            case 0:
                return "&from=" + date + "T00:00:01&to=" + date + "T05:00:00";
            case 1:
                return "&from=" + date + "T05:00:01&to=" + date + "T08:00:00";
            case 2:
                return "&from=" + date + "T08:00:01&to=" + date + "T10:00:00";
            case 3:
                return "&from=" + date + "T10:00:01&to=" + date + "T12:00:00";
            case 4:
                return "&from=" + date + "T12:00:01&to=" + date + "T14:00:00";
            case 5:
                return "&from=" + date + "T14:00:01&to=" + date + "T16:00:00";
            case 6:
                return "&from=" + date + "T16:00:01&to=" + date + "T18:00:00";
            case 7:
                return "&from=" + date + "T18:00:01&to=" + date + "T20:00:00";
            case 8:
                return "&from=" + date + "T20:00:01&to=" + date + "T22:00:00";
            case 9:
                return "&from=" + date + "T22:00:01&to=" + date + "T23:59:59";
            default:
                return "&from=" + date + "&to=" + date;
        }

    }

}
