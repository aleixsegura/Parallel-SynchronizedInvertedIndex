/* ---------------------------------------------------------------
Práctica 3.
Código fuente: SearchFiles.java
Grau Informàtica
Aleix Segura Paz.
Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.io.File;
import java.util.concurrent.CountDownLatch;

public class SearchFiles implements Runnable {
    public static Indexing app;

    private final File[] files;
    private CountDownLatch latch;
    private long fileId = 1L;
    public SearchFiles(File[] files, CountDownLatch latch) {
        this.files = files;
        this.latch = latch;
    }

    /**
     * If the file given is a txt file saves it and if it's a directory processes it recursively in
     * addToTxtFiles function. Only one thread does this task. No need for synchronization.
     */
    @Override
    public void run() {
        for (File file: files){
            if (file.isFile() && isTxtFile(file)) {
                app.getFilesIdsMap().put(fileId, file.getAbsolutePath());
                fileId++;
            } else if (file.isDirectory())
                recursiveSearch(file);
        }
        latch.countDown();
    }

    /**
     * Recursively searches txt files.
     * @param file
     */
    private void recursiveSearch(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) {
                        recursiveSearch(f);
                    } else if (f.isFile() && isTxtFile(f)) {
                        app.getFilesIdsMap().put(fileId, f.getAbsolutePath());
                        fileId++;
                    }
                }
            }
        }
    }

    /**
     * Simply returns if a file is a txt file or not.
     * @param file
     */
    private static boolean isTxtFile(File file) {
        return file.getName().endsWith("txt");
    }
}

