/* ---------------------------------------------------------------
Práctica 3.
Código fuente: ReadFile.java
Grau Informàtica
48056540H - Aleix Segura Paz.
21161168H - Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.io.*;
import java.util.HashMap;

public class ReadFile implements Runnable{
    public static Indexing app;
    private final Long fileId;
    private final File fileToRead;
    private final HashMap<Location, String> filesLinesContent;

    public ReadFile(Long fileId, File fileToRead){
        this.fileId = fileId;
        this.fileToRead = fileToRead;
        filesLinesContent = new HashMap<>();
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
                filesLinesContent.put(location, line);
                lineNumber++;
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public HashMap<Location, String> getLocalFilesLinesContent(){ return filesLinesContent; }

}
