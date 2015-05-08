package de.hpi.dbda;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class Main {
	
	public static boolean compareLists(List<String> l1, List<String> l2) {
		//TODO implement as Hashset @Mascha
		
		if (l1.size() != l2.size())
			return false;
		for (int i = 0; i < l1.size()-1; i++) {
			if (l1.get(i).equals(l2.get(i)) == false)
				return false;
		}
		return true;
	}
	
	public static Set<List<String>> generateCandidates(List<Tuple2<List<String>, Integer>> largeItems) {
		Set<List<String>> allLargeItems=new HashSet<List<String>>(largeItems.size());
		for(Tuple2<List<String>,Integer> tuple:largeItems) {
			allLargeItems.add(tuple._1);
		}
		
		HashSet<List<String>> result = new HashSet<List<String>>();
		MVMap map = new MVMap();
		for (int i = 0; i < largeItems.size(); i++) {
			ListPointer lp = new ListPointer(largeItems.get(i)._1, largeItems.get(i)._1.size() - 1);
			map.put(lp, largeItems.get(i)._1);
		}
		
		for (Object key : map.keySet()) {
			ArrayList<List<String>> entrySet = (ArrayList<List<String>>)map.getCollection(key);
			for (int i = 0; i < entrySet.size(); i++) {
				for (int j = i+1; j < entrySet.size(); j++) {
					List<String> res;
					if (entrySet.get(i).get(entrySet.get(i).size()-1).compareTo(entrySet.get(j).get(entrySet.get(j).size()-1)) < 0) {
						res = new ArrayList<String>(entrySet.get(i));
						res.add(entrySet.get(j).get(entrySet.get(j).size()-1));
					} else {
						res = new ArrayList<String>(entrySet.get(j));
						res.add(entrySet.get(i).get(entrySet.get(i).size()-1));
					}
					
					boolean keepRes=true;
					for(int ignoreIndex = 0; ignoreIndex < res.size() - 2; ignoreIndex++) {
						LinkedList<String> subset = new LinkedList<String>(res);
						subset.remove(ignoreIndex);
						if(!allLargeItems.contains(subset)){
							keepRes=false;
							break;
						}
					}
					if(keepRes){
						result.add(res);
					}
					/* TODO: ausprobieren
					for(int ignoreIndex = 0; ignoreIndex < res.size() - 2; ignoreIndex++) {
						ListPointer searchPointer = new ListPointer(res, ignoreIndex);
						ArrayList<List<String>> valuesFromMap = (ArrayList<List<String>>) map.getCollection(searchPointer);
						for(List<String> value:valuesFromMap){
							for(int compareIndex = 0; compareIndex < value.size(); compareIndex++) {
								if(value.get(compareIndex).equals(anObject))
							}
							
						}
					}
					*/
						
				}
			}
		}
		
		return null;
	}
	
	public static void main(String[] args) {

	if (args.length < 2) {
		System.out.println("please provide the following parameters: input_path and result_file_path");
		System.exit(0);
	}

	SparkConf sparkConf = new SparkConf().setAppName(Main.class.getName()).set("spark.hadoop.validateOutputSpecs", "false");;
	JavaSparkContext context = new JavaSparkContext(sparkConf);

	PairFlatMapFunction<String, List<String>, Integer> mapper = new PairFlatMapFunction<String, List<String>, Integer>() {
		private static final long serialVersionUID = -4625524329716723997L;

		public Iterable<Tuple2<List<String>, Integer>> call(String line) throws Exception {
			HashSet<Tuple2<List<String>, Integer>> result = new HashSet<Tuple2<List<String>, Integer>>();
			
			String[] items = line.split(" ");
			for (String item : items) {
				List<String> itemList = new ArrayList<String>();
				itemList.add(item);
				result.add(new Tuple2<List<String>, Integer>(itemList, 1));
			}
			return result;
		}
		
	};
	
	Function2<Integer, Integer, Integer> reducer = new Function2<Integer, Integer, Integer>() {
		private static final long serialVersionUID = 9090470139360266644L;

		public Integer call(Integer count1, Integer count2) throws Exception {
			return count1 + count2;
		}
	};
	
	Function<Tuple2<List<String>, Integer>, Boolean> minSupportFilter = new Function<Tuple2<List<String>, Integer>, Boolean>() {
		private static final long serialVersionUID = 4372559437013933640L;
		private static final int minSupport = 1000;

		public Boolean call(Tuple2<List<String>, Integer> input) throws Exception {
			return input._2 >= minSupport;
		}
	};
	
	JavaRDD<String> transactions = context.textFile(args[0]);
	JavaPairRDD<List<String>, Integer> transactionsMapped = transactions.flatMapToPair(mapper);
	JavaPairRDD<List<String>, Integer> reducedTransactions = transactionsMapped.reduceByKey(reducer);
	JavaPairRDD<List<String>, Integer> filteredSupport = reducedTransactions.filter(minSupportFilter);
	List<Tuple2<List<String>, Integer>> collected = filteredSupport.collect();
	generateCandidates(collected);
	
	}
}