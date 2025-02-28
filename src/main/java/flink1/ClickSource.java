package flink1;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    private Boolean running = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Bob", "Tom", "Jerry"};
        String[] urls = {"/order", "/home", "/fav", "/cart"};
        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new flink1.Event(user, url, timestamp));
        }
        Thread.sleep(100L);
    }

    @Override
    public void cancel() {
        running = false;
    }
}