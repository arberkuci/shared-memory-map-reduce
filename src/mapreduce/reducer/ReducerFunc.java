package mapreduce.reducer;

import java.util.List;
import mapreduce.util.Tuple;

public interface ReducerFunc<K, V, OutKey, OutVal> {

    public Tuple<OutKey, OutVal> reduce(K key, List<V> value);

}
