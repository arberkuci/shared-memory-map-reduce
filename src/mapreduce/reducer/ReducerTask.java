package mapreduce.reducer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import mapreduce.util.Tuple;

public class ReducerTask<InterKey, InterVal, OutKey, OutVal> implements Callable<List<Tuple<OutKey, OutVal>>> {

    private ReducerFunc<InterKey, InterVal, OutKey, OutVal> reducerFunc;
    private List<Tuple<InterKey, List<InterVal>>> data;

    public ReducerTask(ReducerFunc<InterKey, InterVal, OutKey, OutVal> reducerFunc) {
        this.reducerFunc = reducerFunc;
        this.data = new ArrayList<>();
    }

    @Override
    public List<Tuple<OutKey, OutVal>> call() throws Exception {
        var reduceRes = new ArrayList<Tuple<OutKey, OutVal>>();
        this.data.forEach(tuple -> reduceRes.add(reducerFunc.reduce(tuple.getKey(), tuple.getValue())));
        return reduceRes;
    }

    public void insertTuple(Tuple<InterKey, List<InterVal>> tuple) {
        this.data.add(tuple);
    }

}
