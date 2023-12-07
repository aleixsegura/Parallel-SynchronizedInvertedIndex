/* ---------------------------------------------------------------
Práctica 3.
Código fuente: InvertedIndexBuilder.java
Grau Informàtica
48056540H - Aleix Segura Paz.
21161168H - Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.text.Normalizer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class InvertedIndexBuilder implements Runnable{

    private static final Lock lock = new ReentrantLock(true);
    private static Condition varCond = lock.newCondition();


    // Globals
    private static int globalProcessingFiles = 0;
    private static int globalProcessedFiles = 0;
    private static long globalProcessedLines = 0;
    private static long globalProcessedWords = 0;
    private static long globalProcessedLocations = 0;
    private static long globalUniqueWords = 0;
    private static String globalMostPopularWord = "";
    private static long globalMostPopularWordAppearances = 0;
    private static AtomicBoolean lastIter = new AtomicBoolean(false);

    // Locals
    int processingFiles = 0;
    int processedFiles = 0;
    private long processedLines = 0;
    private long processedWords = 0;
    private long processedLocations = 0;
    private long uniqueWords = 0;
    private String mostPopularWord = "";
    private long mostPopularWordAppearances = 0;

    private static final Runnable printGlobalStatistics = () -> {
        System.out.print(Indexing.STATISTICS_BLUE);
        System.out.println("_____________________________________ " + "[Threads Globals]" + " ___________________________________________________________________________________________");
        System.out.printf("__ Processed Parts of Files:     %-10s__ Processing Parts of Files: %-10s__ Processed Lines:   %-10s__ Processed Words: %-10s__%n", globalProcessedFiles, globalProcessingFiles, globalProcessedLines, globalProcessedWords);
        System.out.printf("__ Processed Locations:          %-10s__ Found Keys:                %-10s__ Most Popular word: %-10s__ Locations:       %-10s__%n", globalProcessedLocations, globalUniqueWords, globalMostPopularWord, globalMostPopularWordAppearances);
        System.out.println("___________________________________________________________________________________________________________________________________________________");
        System.out.print(Indexing.RESET_COLOR);
        if (!lastIter.get())
            resetPartialGlobals();
    };
    private static final CyclicBarrier barrier = new CyclicBarrier(Indexing.threadsSize, printGlobalStatistics);

    private static final Phaser phaser = Indexing.phaser;


    HashMap<Long, Integer> filesToProcessAndPendingWords = new HashMap<>();


    private final int threadId;
    private final Indexing app;
    private final HashMap<Location, String> localFileLinesMap;
    private final HashMap<String, String> localInvertedIndex;

    public InvertedIndexBuilder(int threadId, Indexing app, HashMap<Location, String> localFileLinesMap) {
        this.threadId = threadId;
        this.app = app;
        this.localFileLinesMap = localFileLinesMap;
        localInvertedIndex = new HashMap<>();
    }

    /**
     * For each word in each entry of the localMap we build the inverted index checking if the location is already in
     * the local inverted index or not. Then we add the localInvertedIndex to the globalInvertedIndex.
     */
    @Override
    public void run() {
        calculateNumberOfWordsPerFile();

        for (Map.Entry<Location, String> entry : localFileLinesMap.entrySet()) {
            Location location = entry.getKey();
            long fileId = location.getFileId();

            String toStringLocation = location.toString();
            String line = entry.getValue();

            line = Normalizer.normalize(line, Normalizer.Form.NFD);
            line = line.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
            String filter_line = line.replaceAll("[^a-zA-Z0-9áÁéÉíÍóÓúÚäÄëËïÏöÖüÜñÑ ]", "");
            String[] words = filter_line.split("\\s+");

            StringBuilder locationsBuilder = new StringBuilder(toStringLocation);

            for (String word : words) {
                processedWords++;
                word = word.toLowerCase();

                localInvertedIndex.merge(word, locationsBuilder.toString(), (existing, newLocation) -> {
                    if (!existing.contains(toStringLocation)) {
                        processedLocations++;
                    }
                    return existing.contains(toStringLocation) ? existing : newLocation + " " + existing;
                });

                if (filesToProcessAndPendingWords.containsKey(fileId)){
                    filesToProcessAndPendingWords.compute(fileId, (id, count) -> (count != null) ? count - 1 : 0);
                }
                if (processedWords % app.getM() == 0){
                    try {
                        computeLocalStatistics();
                        showLocalStatistics();
                        accumulateToGlobalStatistics();
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            processedLines++;
        }
        try {
            lastIter.set(true);
            computeLocalStatistics();
            showLocalStatistics();
            accumulateToGlobalStatistics();
            barrier.await();// Wait all threads last final code
            addToGlobalUnsortedInvertedIndex();
            phaser.arriveAndAwaitAdvance(); // Synchronization point
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }

    private void computeMostPopularWord(){
        int appearancesOfMostPopular = 0;
        for (Map.Entry<String, String> wordLocations: localInvertedIndex.entrySet()){
            String word = wordLocations.getKey();
            if (word.trim().isEmpty()) continue;
            int appearances = wordLocations.getValue().split(" ").length;
            if (appearances > appearancesOfMostPopular){
                mostPopularWord = word;
                appearancesOfMostPopular = appearances;
            }
        }
        mostPopularWordAppearances = appearancesOfMostPopular;
    }

    private void computeLocalStatistics(){
        uniqueWords = localInvertedIndex.size();
        computeMostPopularWord();
        processingFiles = computeProcessingFiles();
        processedFiles = computeProcessedFiles();
    }

    private int computeProcessedFiles(){
        int result = 0;
        for (Map.Entry<Long, Integer> entry: filesToProcessAndPendingWords.entrySet()){
            if (entry.getValue() == 0) result++;
        }
        return result;
    }

    private int computeProcessingFiles(){
        int result = 0;
        for (Map.Entry<Long, Integer> entry: filesToProcessAndPendingWords.entrySet()){
            if (entry.getValue() > 0) result++;
        }
        return result;
    }

    private void calculateNumberOfWordsPerFile() {
        for (Map.Entry<Location, String> entry : localFileLinesMap.entrySet()) {
            long id = entry.getKey().getFileId();
            String line = entry.getValue();
            String[] words = line.split("\\s+");

            line = Normalizer.normalize(line, Normalizer.Form.NFD);
            line = line.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
            String filter_line = line.replaceAll("[^a-zA-Z0-9áÁéÉíÍóÓúÚäÄëËïÏöÖüÜñÑ ]", "");

            int wordCount = (int) Arrays.stream(filter_line.split("\\s+")).count();

            if (filesToProcessAndPendingWords.containsKey(id)){
                filesToProcessAndPendingWords.compute(id, (fileId, currentCount) -> (currentCount != null) ? currentCount + wordCount : wordCount);
            } else {
                filesToProcessAndPendingWords.put(id, wordCount);
            }
        }
    }



    private void showLocalStatistics() {
        System.out.print(Indexing.STATISTICS_BLUE);
        System.out.println("_____________________________________ " + "[Thread " + threadId + "] _________________________________________________________________________________________________");
        System.out.printf("__ Processed Parts of Files:     %-10s__ Processing Parts of Files: %-10s__ Processed Lines:   %-10s__ Processed Words: %-10s__%n", processedFiles, processingFiles, processedLines, processedWords);
        System.out.printf("__ Processed Locations:          %-10s__ Found Keys:                %-10s__ Most Popular word: %-10s__ Locations:       %-10s__%n", processedLocations, uniqueWords, mostPopularWord, mostPopularWordAppearances);
        System.out.println("___________________________________________________________________________________________________________________________________________________");
        System.out.print(Indexing.RESET_COLOR);
    }

    // Cal fer que Processed Words, a les estadistiques globals finals sobretot,
    private void accumulateToGlobalStatistics() throws InterruptedException {
        lock.lock();
        globalProcessedFiles += processedFiles;
        globalProcessingFiles += processingFiles;
        globalUniqueWords += uniqueWords;
        globalProcessedLines += processedLines;
        globalProcessedLocations += processedLocations;
        globalProcessedWords += processedWords; // be
        if (mostPopularWordAppearances > globalMostPopularWordAppearances){
            globalMostPopularWord = mostPopularWord;
            globalMostPopularWordAppearances = mostPopularWordAppearances;
        }
        lock.unlock();
    }

    private static void resetPartialGlobals(){
        globalProcessingFiles = 0;
        globalProcessedFiles = 0;
        globalUniqueWords = 0;
        globalProcessedLines = 0;
        globalProcessedLocations = 0;
        globalProcessedWords = 0;
        globalMostPopularWord = "";
        globalMostPopularWordAppearances = 0;
    }

    private void addToGlobalUnsortedInvertedIndex(){
        lock.lock();
        try{
            app.getUnsortedInvertedIndex().add(localInvertedIndex);
            varCond.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public static long getGlobalProcessedLocations() { return globalProcessedLocations;}
    public static long getGlobalProcessedLines() { return globalProcessedLines; }
}
