package flink4;

public class ItemViewCount {

    public String itemId;
    public Long windowEnd;
    public Long count;

    public ItemViewCount() {
    }

    public ItemViewCount(String itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId='" + itemId + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
