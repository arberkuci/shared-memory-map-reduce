package mapreduce.reducer;

import java.util.List;
import java.util.Map.Entry;

@FunctionalInterface
public interface ReducerFunc<K, V, OutKey, OutVal> {

    public Entry<OutKey, OutVal> reduce(K key, List<V> value);

}
