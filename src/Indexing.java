/* ---------------------------------------------------------------
Práctica 3.
Código fuente: Indexing.java
Grau Informàtica
48056540H - Aleix Segura Paz.
21161168H - Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;


public class Indexing {
    public static int threadsSize;
    public static final String STATISTICS_BLUE = "\033[01;34m";
    public static final String RESET_COLOR = "\033[0m";

    private static Indexing app;
    private static final int DEFAULT_M_WORDS = 1000;
    private static final String DEFAULT_INDEX_DIR = "." + File.separator + "Index" + File.separator;

    private final String inputDirPath;
    private final String indexDirPath;
    private final int M;

    private final HashMap<Long, String> filesIdsMap = new HashMap<>(); // filesIds Data Structure
    private final HashMap<Location, String> fileLines = new HashMap<>(); // fileLines Data Structure
    private final ArrayList<HashMap<Location, String>> sameSizeFileLines = new ArrayList<>(); // Rescaled in order to force each virtual thread does the same work.

    private final ArrayList<HashMap<String, String>> unsortedInvertedIndex = new ArrayList<>();
    private HashMap<String, ArrayList<String>> invertedIndex = new HashMap<>(); // invertedIndex Data Structure


    public Indexing(String inputDirPath) {
        this.inputDirPath = inputDirPath;
        this.indexDirPath = DEFAULT_INDEX_DIR;
        this.M = DEFAULT_M_WORDS;
    }

    public Indexing(String inputDirPath, String indexDirPath) {
        this.inputDirPath = inputDirPath;
        this.indexDirPath = indexDirPath;
        this.M = DEFAULT_M_WORDS;
    }

    public Indexing(String inputDirPath, String indexDirPath, int M) {
        this.inputDirPath = inputDirPath;
        this.indexDirPath = indexDirPath;
        this.M = M;
    }

    public static void setApp(Indexing app) { Indexing.app = app; }

    public static void main(String[] args) throws InterruptedException {
        if (args.length == 4) {
            app = new Indexing(args[1], args[2], Integer.parseInt(args[3]));
        } else if (args.length == 3)
            app = new Indexing(args[1], args[2]);
        else if (args.length == 2)
            app = new Indexing(args[1]);
        else
            printUsage();

        Instant start = Instant.now();

        app.searchTxtFiles(app.inputDirPath); // 1 Thread. Busca fitxers txt i fica en estructura dades
        app.saveFileIds(); // 1 Thread. Llegeix de estructura dades i crea el arxiu ID-PATH.TXT (FINS AQUI RES DESTADISTIQUES)

        app.buildFileLinesContent(); // 24 Threads. Envia una tasca FILE_ID-PATH a que cada fil fiqui location(FILE_ID, lineNumber)-lineContent en una estructura dades local
        app.constructFileLinesContent();
        app.constructSameSizeFileLines(); // Mapes de la mateixa mida (Location-lineContent)

        app.constructInvertedIndex();
        app.invertedIndex = app.sortGlobalInvertedIndex();

        app.generateInvertedIndexFiles();
        app.printFinalStatistics();

        Instant end = Instant.now();
        System.out.println("Time to build the Inverted Index: " +
                Duration.between(start, end).toMillis() + " milliseconds.");
    }

    // Getters
    public String getAppInputDir() { return inputDirPath; }
    public String getAppIndexDir() { return indexDirPath; }
    public int getM() { return M;}
    public HashMap<Long, String> getFilesIdsMap() { return filesIdsMap; }
    public HashMap<Location, String> getFileLines() { return fileLines; }
    public ArrayList<HashMap<Location, String>> getSameSizeFileLines() { return sameSizeFileLines;}
    public HashMap<String, ArrayList<String>> getInvertedIndex() { return invertedIndex; }
    public ArrayList<HashMap<String, String>> getUnsortedInvertedIndex() { return unsortedInvertedIndex;}

    /**
     * For each file in inputPath a virtual thread is launched and searches for .txt files. Also removes the 'Input' dir.
     * if existed and creates it again to avoid text overriding.
     *
     * @param inputPath
     */
    public void searchTxtFiles(String inputPath) {
        rmdir();
        mkdir();
        File inputFile = new File(inputPath);
        File[] files = inputFile.listFiles();

        if (files != null) {
            CountDownLatch latch = new CountDownLatch(1);
            SearchFiles.app = app;
            try{
                Thread.startVirtualThread(new SearchFiles(files, latch));
                latch.await();
            } catch (OutOfMemoryError | InterruptedException e){
                assert e instanceof OutOfMemoryError;
                cancel((OutOfMemoryError) e);
            }
        }
    }

    /**
     * Starts a virtual thread which runs the task of saving the full path of the .txt files, and it's id.
     */
    public void saveFileIds() {
        SaveFileIds.app = app;
        SaveFileIds.appIndexDir = app.indexDirPath;
        CountDownLatch latch = new CountDownLatch(1);
        try{
            Thread.startVirtualThread(new SaveFileIds(latch));
            latch.await();
        } catch (OutOfMemoryError | InterruptedException e){
            assert e instanceof OutOfMemoryError;
            cancel((OutOfMemoryError) e);
        }
    }


    static Semaphore semaphore = new Semaphore(0);
    /**
     * Constructs the 'FileIds' txt file which contains lines of the form [id fullTxtFilePath].
     */
    public void buildFileLinesContent() {
        ReadFile.app = app;
        int i = 0, j = 0;
        for (Map.Entry<Long, String> entry : filesIdsMap.entrySet()) {
            Long fileId = entry.getKey();
            String fullPath = entry.getValue();
            ReadFile currentTask = new ReadFile(fileId, new File(fullPath));
            try {
                Thread.startVirtualThread(currentTask); // 2 task
            } catch (OutOfMemoryError e){
                cancel(e);
            }
        }
        try {
            semaphore.acquire(filesIdsMap.size());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Generates FileLinesContent.txt file iterating fileLines global data structure.
     */
    public void constructFileLinesContent() {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(this.indexDirPath + File.separator +
                "FileLinesContent.txt", true))) {
            for (Map.Entry<Location, String> entry : fileLines.entrySet()) {
                bw.write(entry.getKey().toString() + " " + entry.getValue());
                bw.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Rescales the fileLines map in order that the virtual threads that will construct the inverted index can
     * do the same amount of work. This helps the application to run faster.
     */
    public void constructSameSizeFileLines() {
        List<Map.Entry<Location, String>> fileLinesEntries = new ArrayList<>(fileLines.entrySet());
        int totalEntries = fileLinesEntries.size();
        int parts = filesIdsMap.size();
        int entriesPerPart = totalEntries / parts;

        for (int i = 0; i < parts; i++) {
            int beginIndex = i * entriesPerPart;
            int endIndex = (i + 1) * entriesPerPart;

            if (i == parts - 1)
                endIndex = totalEntries;

            List<Map.Entry<Location, String>> partEntries = fileLinesEntries.subList(beginIndex, endIndex);

            HashMap<Location, String> partMap = new HashMap<>();
            for (Map.Entry<Location, String> entry : partEntries) {
                partMap.put(entry.getKey(), entry.getValue());
            }
            sameSizeFileLines.add(partMap);
        }
    }

    static Phaser phaser;
    /**
     * Constructs the final inverted index concurrently.
     */
    public void constructInvertedIndex() {
        threadsSize = filesIdsMap.size();
        phaser = new Phaser(threadsSize + 1);

        int i = 0, j = 0;
        int id = 1;
        for (HashMap<Location, String> map : sameSizeFileLines) {
            InvertedIndexBuilder currentTask = new InvertedIndexBuilder(id++,this, map);
            try{
                Thread.startVirtualThread(currentTask);
            } catch (OutOfMemoryError e){
                cancel(e);
            }
        }
        phaser.arriveAndAwaitAdvance();
    }

    /**
     * Constructs the definitive inverted index data structure that it's a result of iterating all local
     * inverted index maps.
     */
    public HashMap<String, ArrayList<String>> sortGlobalInvertedIndex() {
        HashMap<String, ArrayList<String>> invertedIndex = new HashMap<>();

        for (HashMap<String, String> map : unsortedInvertedIndex) { // iterate each map
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String word = entry.getKey();
                String locations = entry.getValue();

                if (invertedIndex.containsKey(word)) {
                    invertedIndex.get(word).add(locations);
                } else {
                    ArrayList<String> values = new ArrayList<>();
                    values.add(locations);
                    invertedIndex.put(word, values);
                }
            }
        }
        return invertedIndex;
    }

    static CountDownLatch latch = new CountDownLatch(threadsSize);
    /**
     * Generates threads that do the same amount of work and build the IndexFile_x.txt files.
     */
    public void generateInvertedIndexFiles() throws InterruptedException {
        int numberOfFiles = filesIdsMap.size();

        String commonFileName = "IndexFile";
        String directoryPath = this.indexDirPath + File.separator;
        long id = 1L;

        int totalEntries = invertedIndex.size();
        int entriesPerPart = totalEntries / numberOfFiles;

        for (int i = 0; i < numberOfFiles; i++) {
            int beginIndex = i * entriesPerPart;
            int endIndex = (i + 1) * entriesPerPart;

            if (i == numberOfFiles - 1) // if lastFile -> process pending work
                endIndex = totalEntries;

            String filename = commonFileName + "_" + id++;
            List<Map.Entry<String, ArrayList<String>>> partInvertedIndex = new ArrayList<>
                    (invertedIndex.entrySet()).subList(beginIndex, endIndex);
            try{
                Thread.startVirtualThread(new IndexFileBuilder(directoryPath, filename, partInvertedIndex));
            } catch (OutOfMemoryError e){
                cancel(e);
            }
        }
        latch.await();
    }

    /**
     * Constructs the directory in which we store the resulting files.
     */
    private void mkdir() {
        File indexDirFile = new File(this.indexDirPath);
        if (!indexDirFile.exists()) {
            if (!indexDirFile.mkdirs()) {
                System.err.println("Directory: " + this.indexDirPath + " could not be created.");
            }
        }
    }

    /**
     * Deletes a file invoking recursiveDelete method.
     */
    private void rmdir() {
        File indexDir = new File(this.indexDirPath);
        if (indexDir.exists()) {
            recursiveDelete(indexDir);
        }
    }

    /**
     * Recursively deletes a file.
     * @param file root file to delete recursively.
     */
    private void recursiveDelete(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File child : files) {
                    recursiveDelete(child);
                }
            }
        }
        file.delete();
    }


    /**
     * Simply prints usage if the user has introduced wrong arguments.
     */
    private static void printUsage() {
        System.err.println("Error in parameters. At least one argument (source directory) is needed.");
        System.err.println("Usage: Indexing --enable-preview <SourceDirectory> [<IndexDirectory>] [<StatisticsAfterM_Words>]");
        System.exit(1);
    }



    private static void cancel(OutOfMemoryError e){
        e.printStackTrace();
        System.exit(1);
    }

    int mostPopularFinalWordAppearances = 0;
    private void printFinalStatistics(){
        System.out.print(STATISTICS_BLUE);
        System.out.println("_____________________________________ " + "[Finals]" + " ____________________________________________________________________________________________________");
        System.out.printf("__ Processed Files:              %-10s__ Processing Files:          %-10s__ Processed Lines:   %-10s__ Processed Words: %-10s__%n", app.getFilesIdsMap().size(), 0, InvertedIndexBuilder.getGlobalProcessedLines(), app.invertedIndex.size());
        System.out.printf("__ Processed Locations:          %-10s__ Found Keys:                %-10s__ Most Popular word: %-10s__ Locations:       %-10s__%n", InvertedIndexBuilder.getGlobalProcessedLocations(), app.invertedIndex.size(), app.computeFinalMostPopularWord(), mostPopularFinalWordAppearances);
        System.out.println("___________________________________________________________________________________________________________________________________________________");
        System.out.print(RESET_COLOR);
    }

    private String computeFinalMostPopularWord() {
        HashMap<String, Integer> wordLocationCount = new HashMap<>();

        for (HashMap<String, String> invertedIndex : unsortedInvertedIndex) {
            for (String word : invertedIndex.keySet()) {
                String locations = invertedIndex.get(word);
                int count = locations.split(",").length;

                wordLocationCount.put(word, wordLocationCount.getOrDefault(word, 0) + count);
            }
        }

        String wordWithMostLocations = null;
        int maxLocations = 0;
        for (String word : wordLocationCount.keySet()) {
            int locations = wordLocationCount.get(word);
            if (locations > maxLocations) {
                maxLocations = locations;
                wordWithMostLocations = word;
            }
        }
        mostPopularFinalWordAppearances = maxLocations;
        return wordWithMostLocations;
    }
}
