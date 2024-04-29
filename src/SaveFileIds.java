/* ---------------------------------------------------------------
Práctica 3.
Código fuente: SaveFileIds.java
Grau Informàtica
Aleix Segura Paz.
Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SaveFileIds implements Runnable {
    public static Indexing app;
    public static String appIndexDir;

    private CountDownLatch latch;
    public SaveFileIds(CountDownLatch latch){ this.latch = latch; }

    /**
     * Simply writes in FileIds.txt the full path and the identifier of every txt file encountered. Only 1 thread
     * does this task. No need for synchronization.
     */
    @Override
    public void run() {
        try (BufferedWriter bw =
                     new BufferedWriter(new FileWriter(appIndexDir + File.separator + "FileIds.txt",
                             true))) {
            for (Map.Entry<Long, String> entry : app.getFilesIdsMap().entrySet()){
                Long fileId = entry.getKey();
                String fullPath = entry.getValue();
                String line = fileId + " " + fullPath;
                bw.write(line);
                bw.newLine();
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        latch.countDown();
    }
}

