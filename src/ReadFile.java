/* ---------------------------------------------------------------
Práctica 3.
Código fuente: ReadFile.java
Grau Informàtica
Aleix Segura Paz.
Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.io.*;


public class ReadFile implements Runnable{
    public static Indexing app;
    private final Long fileId;
    private final File fileToRead;

    public ReadFile(Long fileId, File fileToRead){
        this.fileId = fileId;
        this.fileToRead = fileToRead;
    }

    /**
     * Simply adds to the local fileLinesContent map the location of the line, and it's actual content.
     */
    @Override
    public void run(){
        try (BufferedReader br = new BufferedReader(new FileReader(fileToRead))){
            String line;
            Long lineNumber = 1L;
            while ((line = br.readLine()) != null){
                Location location = new Location(fileId, lineNumber);
                synchronized (app.getFileLines()){
                    app.getFileLines().put(location, line);
                    lineNumber++;
                }
            }
            Indexing.semaphore.release();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
