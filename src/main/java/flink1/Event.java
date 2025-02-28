package flink1;

public class Event {

    public String url;
    public String user;
    public Long timestamp;

    public Event(String user, String url, long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    public Event(){}

    public String toString() {
        return "Event " + user + " url = " + url + " timestamp= " + timestamp;
    }
}