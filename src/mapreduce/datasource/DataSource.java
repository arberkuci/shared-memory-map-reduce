package mapreduce.datasource;

import java.util.Map;

public abstract class DataSource<K, V> {

    public abstract Map<K, V> read();

}
