import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

// Tests will be executed for the Big Example (All dirs in Input)
public class IndexingTest {
    String inputDir = "." + File.separator + "Input" + File.separator;
    String indexDir = "." + File.separator + "Index" + File.separator;
    Indexing app = new Indexing(inputDir, indexDir);
    @Test
    public void testDirs(){
        assertEquals(app.getAppInputDir(), inputDir);
        assertEquals(app.getAppIndexDir(), indexDir);
    }
    @Test
    public void testNumOfFiles() throws InterruptedException {
        Indexing.setApp(app);
        app.searchTxtFiles(app.getAppInputDir());
        app.saveFileIds();
        assertEquals(37, app.getFilesIdsMap().size());
    }
}
