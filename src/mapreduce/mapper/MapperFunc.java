package mapreduce.mapper;

import java.util.Map.Entry;

import java.util.List;

@FunctionalInterface
public interface MapperFunc<K, V, OutKey, OutVal> {

    public List<Entry<OutKey, OutVal>> map(K key, V value);

}
