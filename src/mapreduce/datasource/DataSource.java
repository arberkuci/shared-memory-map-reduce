package mapreduce.datasource;

import mapreduce.util.Tuple;

import java.util.List;

public abstract class DataSource<K, V> {

    public abstract List<Tuple<K, V>> read();

}
