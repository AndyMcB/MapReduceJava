import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by AMCBR on 14/03/2017.
 */
public class MapReduce {

    // the problem:

    //1: The program takes a list of text files to be processed.
    // The file list can be passed to the program via the command line. Use a number of big text or log files for testing your program.

    //2: Using Approach 3 (multithreaded) in the attached source code modify the given Map Reduce algorithm to build an output data
    // structure that shows how many words in each file begin with each letter of the alphabet
    // e.g. A => (file1.txt, = 2067, file2.txt = 180, ...), B => (file1.txt = 1234, file2.txt = 235, ...) etc
    // then print out the results. The results can also be written out to a file for later analysis etc.

    //3: Modify the main part of the program to assign the Map or Reduce functions to a Thread Pool with a configurable number of threads.
    // Lookup the Java concurrency utilities for examples of using Thread Pools. The actual number of threads can be passed to the program
    // as a command line parameter.

    final static HashMap<String, String> fileContentsMap = new HashMap<>();
    final static Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
    static ExecutorService executor = null;
    static Logger log = Logger.getLogger(MapReduce.class.getName());


    public static void main(String args[]) throws InterruptedException {
        log.setLevel(Level.INFO);
        log.info("Starting...");

        if (args.length < 2){
             log.severe("Error: Incorrect number of args," +
                     "\n\tUsage: java -jar ct414-1.0-SNAPSHOT <path to text files> <No. of threads (Default=2)>");

            System.exit(1);
        }

        log.info("Num args: " + args.length);

        ArrayList<String> file_paths = new ArrayList<>();
        int numThreads = 2; //Default number of threads will be two if not set
        for(int i=0; i<args.length; i++){
            try{
               numThreads = Integer.parseInt(args[i]);
            }catch(NumberFormatException e){ //Discard error. If the arg isnt a number it should be a file
                file_paths.add(args[i]);
            }
        }

        log.info("Num Files: " + file_paths.size());
        log.info("Num Threads: " + numThreads);

        //Setup
        log.info("Retrieving file contents\n Setting up threadpool ");
        MapReduce mr = new MapReduce(numThreads);
        file_paths.forEach(mr::getFileContents);

        //Map
        log.info("Mapping file contents");
        final List<MappedItem> mappedItems = new LinkedList<>();

        final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() { //MapCallback implementation
            @Override
            public synchronized void mapDone(String key, List<MappedItem> results) {
                mappedItems.addAll(results);
            }
        };

        Collection<Future<?>> futures = new LinkedList<>();
        for( Map.Entry<String, String> entry : fileContentsMap.entrySet()){
            final String file = entry.getKey();
            final String content = entry.getValue();

            futures.add(executor.submit(() -> map(file, content, mapCallback)));
        }

        try { //force the executor to execute all mapping tasks
            for (Future f : futures)
                f.get();
        } catch (ExecutionException e) {e.printStackTrace();}


        //Group
        log.info("Grouping contents");
        Map<String, List<String>> groupedItems = new HashMap<>();

        for ( MappedItem item : mappedItems) {
            String letter = item.getLetter();
            String file = item.getFile();
            List<String> list = groupedItems.get(letter);

            if (list == null){
                list = new LinkedList<String>();
                groupedItems.put(letter, list);
            }
            list.add(file);
        }



        //Reduce
        log.info("Reducing contents");
        final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() { //ReduceCallback implementation
            @Override
            public synchronized void reduceDone(String k, Map<String, Integer> v) {
                output.put(k, v);
            }
        };

        futures.clear();
        for ( Map.Entry<String, List<String>> entry : groupedItems.entrySet() ){
            final String letter = entry.getKey();
            final List<String> list = entry.getValue();

            futures.add(executor.submit(()  -> reduce(letter, list, reduceCallback)));
        }

        log.config("Terminating threadpool");
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        log.info("\n---------------------------------------------Output---------------------------------------------\n");
        for ( Map.Entry<String, Map<String, Integer>> set : output.entrySet())
            System.out.println(set.getKey() + " => " + set.getValue());

        log.info("Outputting to file");
        mr.outputToFile();
    }


    public MapReduce(int poolSize) {

        executor = Executors.newFixedThreadPool(poolSize);
    }


    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        String[] letters = new String[words.length];

        for(int i=0; i< words.length; i++) {
            letters[i] = String.valueOf(words[i].charAt(0));
        }

        List<MappedItem> results = new ArrayList<>(words.length);
        for (String letter : letters) {
            results.add(new MappedItem(letter, file));
        }
        callback.mapDone(file, results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for (String file : list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }


    /**
     * Pretty print the contents of fileContentMap
     */
    public void printFileContents() {

        String out = "";

        for(String key : fileContentsMap.keySet()){

            String content = key + ": " + fileContentsMap.get(key)+"\n";
            out += content;
        }

        System.out.println(out);
    }

    /***
     * Output program output to file
     *
     */
    public void outputToFile(){

        try (BufferedWriter br = new BufferedWriter(new FileWriter("output.txt"))) {

            br.write("\r\n---------------------------------------------Output---------------------------------------------\r\n");
            for (Map.Entry<String, Map<String, Integer>> set : output.entrySet())
                br.write("\r\n"+set.getKey() + " => " + set.getValue());

        }catch(IOException e){e.printStackTrace();}
    }

    /**
     * Add file name and contents to hashmap
     *
     * @param filename
     */
    public void getFileContents(String filename) {

        try (BufferedReader br = new BufferedReader(new FileReader(filename))){

            String curLine = "";
            StringBuffer sb = new StringBuffer();

            while ((curLine = br.readLine()) != null) {
                sb.append(curLine);
            }

            fileContentsMap.put(filename, sb.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public interface MapCallback<E, V> {

        void mapDone(E key, List<V> values);
    }

    public interface ReduceCallback<E, K, V> {

        void reduceDone(E e, Map<K, V> results);
    }

    private static class MappedItem {

        private final String letter;
        private final String file;

        public MappedItem(String letter, String file) {
            this.letter = letter;
            this.file = file;
        }

        public String getLetter() {
            return letter;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + letter + "\",\"" + file + "\"]";
        }
    }



}
