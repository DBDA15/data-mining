import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.collections.map.MultiValueMap;


public class createNetflixTransaction {

	public static MultiValueMap create(String filename) throws IOException {
		MultiValueMap transaction = new MultiValueMap();
		
		FileInputStream fstream = new FileInputStream(filename);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		String strLine;
		String[] fn = filename.split("/");
		String uidWithTxt = fn[fn.length-1];
		String uidWithMV = uidWithTxt.split("_")[1];
		String uidWith = uidWithTxt.split("\\.")[0];
		String uid = uidWith.split("_")[1];
		
		int movieId = Integer.parseInt(uid);
		

		while ((strLine = br.readLine()) != null)   {
			 String[] sentence = strLine.split(",");
			  if (sentence.length != 3)
				  continue;
			  
			  transaction.put(sentence[1], ""+movieId);
		}
		br.close();
		return transaction;

	}
	
	public static void forAllFilesIn(String directory, String outputFile) throws IOException {
		File[] files = new File(directory).listFiles();
		 
		System.out.println(files.length);
		int i = 0;
		for (File file : files) {
			if (i%200 == 0) System.out.print(i + "\n");
		    if (file.isFile()) {
		    	MultiValueMap m = create(file.toString());
		    	writeMapToFile(outputFile, m);
		    	i++;
		    }
		}
	}
	
	
	public static void writeMapToFile(String filename, MultiValueMap transaction) throws IOException{
		File file = new File(filename);
		 
		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
		BufferedWriter bw = new BufferedWriter(fw);
		
		for (Object k : transaction.keySet()) {
			Collection<String> values = transaction.getCollection(k);
			HashSet<String> valueSet = new HashSet<String>(values);
			
			bw.write(k.toString()+" ");
			for (String v : valueSet) {
				bw.write(v);
				bw.write(" ");
			}
			bw.write("\n");
		}
		
		bw.close();
	}
		
	public static void main(String[] args) throws IOException {
		
		forAllFilesIn("/home/mascha/dbda2015/Data/netflix/training_set/", "output3.txt");
		//MultiValueMap m = create("mv_0000001.txt");
		
		//writeMapToFile("user1", m);
	}

}
