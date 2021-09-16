package mapreduce.mapper;

import java.util.Map.Entry;
import java.util.List;
import java.util.concurrent.Callable;

public class MapperTask<InKey, InVal, OutKey, OutVal> implements Callable<List<Entry<OutKey, OutVal>>> {

    private MapperFunc<InKey, InVal, OutKey, OutVal> mapper;
    private InKey inputKey;
    private InVal inputValue;

    public MapperTask(MapperFunc<InKey, InVal, OutKey, OutVal> mapper, InKey inputKey, InVal inputValue) {
        this.mapper = mapper;
        this.inputKey = inputKey;
        this.inputValue = inputValue;
    }

    @Override
    public List<Entry<OutKey, OutVal>> call() throws Exception {
        return mapper.map(inputKey, inputValue);
    }

}
