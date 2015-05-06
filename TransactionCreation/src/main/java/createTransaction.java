import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashSet;
import java.net.URLDecoder;


import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.lang3.StringUtils;

public class createTransaction {

	public static MultiValueMap writeTransactionsToMap(String filename) throws IOException {
		MultiValueMap transaction = new MultiValueMap();
		
		FileInputStream fstream = new FileInputStream(filename);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		String strLine;

		while ((strLine = br.readLine()) != null)   {
		  String[] sentence = strLine.split(" ");
		  if (sentence.length != 4)
			  continue;
		  String subject = StringUtils.substringBetween(sentence[0], "resource/", ">");
		  String resourceName = URLDecoder.decode(subject,"UTF-8");
		  
		  String[] object = sentence[1].split("/");
		  String objectName = object[object.length-1];
		  objectName = URLDecoder.decode(objectName.replace(">", ""), "UTF-8");
		  
		  transaction.put(resourceName, objectName);		  
		}

		br.close();
		System.out.print("read ready");
		return transaction;
	}
	
	public static void writeMapToFile(String filename, MultiValueMap transaction) throws IOException{
		File file = new File(filename);
		 
		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
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
		MultiValueMap m = writeTransactionsToMap("_data_rdf_dbpedia_dbpedia-3.8_geo_coordinates_en.nt");
		writeMapToFile("transactions_geo_coordinates_en", m);
	}

}
