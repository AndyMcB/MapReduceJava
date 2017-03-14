import java.io.*;
import java.util.HashMap;

/**
 * Created by AMCBR on 14/03/2017.
 */
public class MapReduce {

    final HashMap<String, String> fileContentsMap = new HashMap<>();

    public MapReduce() {

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



    public static void main(String args[]) {

        MapReduce mr = new MapReduce();
        mr.getFileContents("test1.txt");
        mr.getFileContents("test2.txt");
        mr.getFileContents("test3.txt");

        mr.printFileContents();
    }

}
