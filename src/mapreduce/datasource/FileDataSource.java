package mapreduce.datasource;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import mapreduce.util.Tuple;

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
    public List<Tuple<Long, String>> read() {
        var splits = new ArrayList<Tuple<Long, String>>(THREAD_COUNT);
        StringBuilder fileContent = new StringBuilder();
        try (Reader reader = new FileReader(this.filePath)) {
            int ch;
            while ((ch = reader.read()) != -1) fileContent.append((char) ch);
        } catch (IOException e) {
            e.printStackTrace();
            // TODO: Handle the exception properly.
        }
        int splitSize = fileContent.length() / THREAD_COUNT;
        int remainder = 0;
        for (int i = 0; i < THREAD_COUNT; i++) {
            StringBuilder currSplit = new StringBuilder();
            int start = i * splitSize + remainder;
            int end = start + splitSize;
            for (; start < end && start < fileContent.length(); start++)
                currSplit.append(fileContent.charAt(start));
            //Go till the end of the current word.
            while (start < fileContent.length() && fileContent.charAt(start) != ' ') {
                currSplit.append(fileContent.charAt(start));
                remainder++;
                start++;
            }
            splits.add(new Tuple<>(Long.valueOf(i), currSplit.toString()));
        }
        return splits;
    }

}
