package io.github.ppolushkin.newsAnalizer.output;

import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class CsvFileWriter {

    @SneakyThrows
    public void write(String fileName, String topics, Map<String, AtomicInteger> topicsCount) {
        StringBuilder r = new StringBuilder();
        Arrays.stream(topics.split(",")).forEach(t -> r.append(topicsCount.get(t).get()).append(","));
        String result = r.toString();
        result = result.substring(0, result.length() - 1);

        File csvOutputFile = new File(fileName);
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            pw.println(topics);
            pw.println(result);
        }
    }

}
