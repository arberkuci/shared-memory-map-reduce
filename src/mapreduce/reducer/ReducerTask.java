package mapreduce.reducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

public class ReducerTask<InterKey, InterVal, OutKey, OutVal> implements Callable<List<Entry<OutKey, OutVal>>> {

    private ReducerFunc<InterKey, InterVal, OutKey, OutVal> reducerFunc;
    private List<Entry<InterKey, List<InterVal>>> data;

    public ReducerTask(ReducerFunc<InterKey, InterVal, OutKey, OutVal> reducerFunc) {
        this.reducerFunc = reducerFunc;
        this.data = new ArrayList<>();
    }

    @Override
    public List<Entry<OutKey, OutVal>> call() throws Exception {
        var reduceRes = new ArrayList<Entry<OutKey, OutVal>>();
        data.forEach(tuple -> reduceRes.add(reducerFunc.reduce(tuple.getKey(), tuple.getValue())));
        return reduceRes;
    }

    public void insertEntry(Entry<InterKey, List<InterVal>> tuple) {
        this.data.add(tuple);
    }

}
