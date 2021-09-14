package mapreduce.mapper;

import mapreduce.util.Tuple;

import java.util.List;

@FunctionalInterface
public interface MapperFunc<K, V, OutKey, OutVal> {

    public List<Tuple<OutKey, OutVal>> map(K key, V value);

}
