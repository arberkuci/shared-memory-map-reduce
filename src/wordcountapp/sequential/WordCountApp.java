package wordcountapp.sequential;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class WordCountApp {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java <<filePath>>");
            return;
        }
        long start = System.currentTimeMillis();
        StringBuilder fileContent = new StringBuilder();
        try (Reader reader = new FileReader(args[0])) {
            int ch;
            while ((ch = reader.read()) != -1) fileContent.append((char) ch);
        } catch (IOException e) {
            e.printStackTrace();
            // TODO: Handle the exception properly.
        }
        String file = fileContent.toString();
        Map<String, Integer> wordCounts = new HashMap<>();
        StringTokenizer stringTokenizer = new StringTokenizer(file, " \\t\\n\\r\\f,.!?;:][()_=+-<>'”");
        while (stringTokenizer.hasMoreTokens()) {
            String next = stringTokenizer.nextToken();
            if (wordCounts.containsKey(next)) {
                wordCounts.merge(next, 1, (x, y) -> x + y);
            } else wordCounts.put(next, 1);
        }
        long end = System.currentTimeMillis();
        System.out.println("Took in total: " + (end - start) / 1000);
    }

}
