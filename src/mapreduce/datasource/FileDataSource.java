package mapreduce.datasource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static mapreduce.util.Constants.THREAD_COUNT;

public class FileDataSource extends DataSource<Long, String> {

    private final String filePath;

    public FileDataSource(String filePath) throws IOException {
        Objects.requireNonNull(filePath);
        requireReadableRegularFile(filePath);
        this.filePath = filePath;
    }

    private void requireReadableRegularFile(String fileName) throws IOException {
        Path path = Paths.get(fileName);
        boolean regularReadableFile = Files.isRegularFile(path) && Files.isReadable(path);
        if (!regularReadableFile) throw new IOException("The file is not readable or not a regular file: " + fileName);
    }

    @Override
    public Map<Long, String> read() {
        var splits = new HashMap<Long, String>();
        StringBuilder fileContent = new StringBuilder();
        try (Reader reader = new FileReader(this.filePath)) {
            int ch;
            while ((ch = reader.read()) != -1) fileContent.append((char) ch);
        } catch (IOException e) {
            e.printStackTrace();
            // TODO: Handle the exception properly.
        }
        //Prepare the document for mapper tasks by splitting
        //it into as many tasks as are there.
        int splitSize = fileContent.length() / THREAD_COUNT;
        Set<Character> delimiters = getDelimiters();
        for (int i = 0; i < THREAD_COUNT; i++) {
            int start = i * splitSize;
            int end = start + splitSize;
            while (end < fileContent.length() && !delimiters.contains(fileContent.charAt(end))) end++;
            splits.put(Long.valueOf(i), fileContent.substring(start, end));
        }
        return splits;
    }

    private Set<Character> getDelimiters() {
        Set<Character> delimiters = new HashSet<>();
        delimiters.add(' ');
        delimiters.add('\t');
        delimiters.add('\n');
        delimiters.add('\r');
        delimiters.add('\f');
        delimiters.add(',');
        delimiters.add('.');
        delimiters.add('!');
        delimiters.add('?');
        delimiters.add(';');
        delimiters.add(':');
        delimiters.add('[');
        delimiters.add(']');
        delimiters.add('(');
        delimiters.add(')');
        delimiters.add('_');
        delimiters.add('=');
        delimiters.add('+');
        delimiters.add('-');
        delimiters.add('<');
        delimiters.add('>');
        delimiters.add('\'');
        delimiters.add('”');
        delimiters.add('"');
        return delimiters;
    }

}
