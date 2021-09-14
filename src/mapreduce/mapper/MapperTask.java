package mapreduce.mapper;

import mapreduce.util.Tuple;

import java.util.List;
import java.util.concurrent.Callable;

public class MapperTask<InKey, InVal, OutKey, OutVal> implements Callable<List<Tuple<OutKey, OutVal>>> {

    private MapperFunc<InKey, InVal, OutKey, OutVal> mapper;
    private Tuple<InKey, InVal> data;

    public MapperTask(MapperFunc<InKey, InVal, OutKey, OutVal> mapper, Tuple<InKey, InVal> data) {
        this.mapper = mapper;
        this.data = data;
    }

    @Override
    public List<Tuple<OutKey, OutVal>> call() throws Exception {
        return mapper.map(data.getKey(), data.getValue());
    }

}
