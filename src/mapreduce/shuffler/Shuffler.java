package mapreduce.shuffler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

public class Shuffler<K, V> implements Runnable {

    private ConcurrentMap<K, List<V>> shuffledData;
    private List<Entry<K, V>> data;
    private CountDownLatch countDownLatch;

    public Shuffler(ConcurrentMap<K, List<V>> shuffledData, List<Entry<K, V>> data, CountDownLatch countDownLatch) {
        this.shuffledData = shuffledData;
        this.data = data;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        for (var entry : data) {
            if (shuffledData.containsKey(entry.getKey())) {
                shuffledData.get(entry.getKey()).add(entry.getValue());
            } else {
                //TODO: Not thread safe.
                var newList = Collections.synchronizedList(new ArrayList<V>());
                newList.add(entry.getValue());
                shuffledData.put(entry.getKey(), newList);
            }
        }
        countDownLatch.countDown();
    }
}
