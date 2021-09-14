package wordcount.parallel.application;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import mapreduce.MapReduce;
import mapreduce.util.Tuple;

public class WordCount {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java filePath");
            return;
        }
        var mapReduce = new MapReduce<Long, String, String, Integer, String, Integer>(args[0]);
        mapReduce.supplyMapper((id, docSplit) -> {
            var wordCountRes = new ArrayList<Tuple<String, Integer>>();
            StringTokenizer stringTokenizer = new StringTokenizer(docSplit, " \\t\\n\\r\\f,.!?;:][()_=+-<>'”");
            while (stringTokenizer.hasMoreTokens())
                wordCountRes.add(new Tuple<>(stringTokenizer.nextToken(), 1));
            return wordCountRes;
        });
        mapReduce.supplyReducer((word, counts) -> new Tuple(word, counts.stream().count()));
        mapReduce.compute();
        List<Tuple<String, Integer>> finalOutput = mapReduce.collectOutput();
        System.out.println(finalOutput);
    }

}
