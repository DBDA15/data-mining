package de.hpi.dbda;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
		// TODO implement as Hashset @Mascha

		if (l1.size() != l2.size())
			return false;
		for (int i = 0; i < l1.size() - 1; i++) {
			if (l1.get(i).equals(l2.get(i)) == false)
				return false;
		}
		return true;
	}

	public static Set<List<String>> generateCandidates(List<Tuple2<List<String>, Integer>> largeItemTuples) {
		Set<List<String>> largeItems = new HashSet<List<String>>(largeItemTuples.size());
		for (Tuple2<List<String>, Integer> tuple : largeItemTuples) {
			largeItems.add(tuple._1);
		}

		HashSet<List<String>> result = new HashSet<List<String>>();
		MultiValueMapWithArrayList map = new MultiValueMapWithArrayList();
		for (int i = 0; i < largeItemTuples.size(); i++) {
			ListPointer key = new ListPointer(largeItemTuples.get(i)._1, largeItemTuples.get(i)._1.size() - 1);
			map.put(key, largeItemTuples.get(i)._1);
		}

		for (Object key : map.keySet()) {
			ArrayList<List<String>> entrySet = (ArrayList<List<String>>) map.getCollection(key);
			for (int i = 0; i < entrySet.size() - 1; i++) {
				for (int j = i + 1; j < entrySet.size(); j++) {
					List<String> candidate;
					if (entrySet.get(i).get(entrySet.get(i).size() - 1).compareTo(entrySet.get(j).get(entrySet.get(j).size() - 1)) < 0) { // if the item set at position i is "smaller than" the item set at position j
						candidate = new ArrayList<String>(entrySet.get(i));
						candidate.add(entrySet.get(j).get(entrySet.get(j).size() - 1));
					} else {
						candidate = new ArrayList<String>(entrySet.get(j));
						candidate.add(entrySet.get(i).get(entrySet.get(i).size() - 1));
					}

					boolean keepCandidate = true;
					for (int ignoreIndex = 0; ignoreIndex < candidate.size() - 2; ignoreIndex++) {
						LinkedList<String> subset = new LinkedList<String>(candidate);
						subset.remove(ignoreIndex);
						if (!largeItems.contains(subset)) {
							keepCandidate = false;
							break;
						}
					}
					if (keepCandidate) {
						result.add(candidate);
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

		return result;
	}

	public static Set<IntArray> generateCandidatesInt(List<Tuple2<IntArray, Integer>> largeItemTuples) {
		Set<IntArray> largeItems = new HashSet<IntArray>(largeItemTuples.size());
		for (Tuple2<IntArray, Integer> tuple : largeItemTuples) {
			largeItems.add(tuple._1);
		}

		HashSet<IntArray> result = new HashSet<IntArray>();
		MultiValueMapWithArrayList map = new MultiValueMapWithArrayList();
		for (int i = 0; i < largeItemTuples.size(); i++) {
			IntArrayPointer key = new IntArrayPointer(largeItemTuples.get(i)._1, largeItemTuples.get(i)._1.value.length - 1);
			map.put(key, largeItemTuples.get(i)._1);
		}

		for (Object key : map.keySet()) {
			ArrayList<IntArray> entrySet = (ArrayList<IntArray>) map.getCollection(key);
			for (int i = 0; i < entrySet.size() - 1; i++) {
				for (int j = i + 1; j < entrySet.size(); j++) {
					IntArray candidate;
					if (entrySet.get(i).value[entrySet.get(i).value.length - 1] < entrySet.get(j).value[entrySet.get(j).value.length - 1]) { // if the item set at position i is "smaller than" the item set at position j
						candidate = new IntArray(entrySet.get(i), entrySet.get(j).value[entrySet.get(j).value.length - 1]);
					} else {
						candidate = new IntArray(entrySet.get(j), entrySet.get(i).value[entrySet.get(i).value.length - 1]);
					}

					boolean keepCandidate = true;
					for (int ignoreIndex = 0; ignoreIndex < candidate.value.length - 2; ignoreIndex++) {
						// store a copy of candidate minus the element at ignoreIndex in newValue
						int[] subset = new int[candidate.value.length - 1];
						for (int copyIndex = 0, newValueIndex = 0; newValueIndex < subset.length; copyIndex++, newValueIndex++) {
							if (copyIndex == ignoreIndex) {
								copyIndex++;
							}
							subset[newValueIndex] = candidate.value[copyIndex];
						}

						if (!largeItems.contains(new IntArray(subset))) {
							keepCandidate = false;
							break;
						}
					}
					if (keepCandidate) {
						result.add(candidate);
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

		return result;
	}

	private static List<IntArray> intCompressInputFile(String inputPath) throws IOException {
		Map<String, Integer> compressionMap = new HashMap<String, Integer>();
		BufferedReader reader = new BufferedReader(new FileReader(inputPath));
		String line;
		String[] words;
		int compressionOutput = Integer.MIN_VALUE;
		while ((line = reader.readLine()) != null) {
			words = line.split(" ");
			for (String myWord : words) {
				compressionMap.put(myWord, compressionOutput);
				compressionOutput++;
			}
		}
		reader.close();

		reader = new BufferedReader(new FileReader(inputPath));
		ArrayList<IntArray> compressedFile = new ArrayList<IntArray>();
		int[] compressedLine;
		while ((line = reader.readLine()) != null) {
			words = line.split(" ");
			compressedLine = new int[words.length];
			for (int i = 0; i < words.length; i++) {
				compressionOutput = compressionMap.get(words[i]);
				compressedLine[i] = compressionOutput;
			}
			compressedFile.add(new IntArray(compressedLine));
		}
		reader.close();
		return compressedFile;
	}

	private static void aprioriOnInts(String[] args, JavaSparkContext context) throws IOException {
		Function<IntArray, IntArray> transactionParser = new Function<IntArray, IntArray>() {
			private static final long serialVersionUID = 165521644289765913L;

			public IntArray call(IntArray items) throws Exception {
				Arrays.sort(items.value);
				return items;
			}

		};

		PairFlatMapFunction<IntArray, IntArray, Integer> singleItemsExtractor = new PairFlatMapFunction<IntArray, IntArray, Integer>() {
			private static final long serialVersionUID = -2714321550777030577L;

			public Iterable<Tuple2<IntArray, Integer>> call(IntArray transaction) throws Exception {
				HashSet<Tuple2<IntArray, Integer>> result = new HashSet<Tuple2<IntArray, Integer>>();

				for (int item : transaction.value) {
					IntArray itemList = new IntArray(new int[] { item });
					result.add(new Tuple2<IntArray, Integer>(itemList, 1));
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

		Function<Tuple2<IntArray, Integer>, Boolean> minSupportFilter = new Function<Tuple2<IntArray, Integer>, Boolean>() {
			private static final long serialVersionUID = 1188423613305352530L;
			private static final int minSupport = 1000;

			public Boolean call(Tuple2<IntArray, Integer> input) throws Exception {
				return input._2 >= minSupport;
			}
		};

		boolean firstRound = true;
		Set<IntArray> candidates = null;

		long startTime = System.currentTimeMillis();
		List<IntArray> compressedInputFile = intCompressInputFile(args[0]);
		System.out.println("compressing the input file on the master took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
		System.out.println("Memory in use [MB]: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024);

		startTime = System.currentTimeMillis();
		JavaRDD<IntArray> inputLines = context.parallelize(compressedInputFile);
		System.out.println("parallelizing the compressed input file took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");

		startTime = System.currentTimeMillis();
		JavaRDD<IntArray> transactions = inputLines.map(transactionParser);

		do {
			JavaPairRDD<IntArray, Integer> transactionsMapped;
			if (firstRound) {
				firstRound = false;
				transactionsMapped = transactions.flatMapToPair(singleItemsExtractor);
			} else {
				CandidateMatcherInt myCandidateMatcher = new CandidateMatcherInt(candidates);
				transactionsMapped = transactions.flatMapToPair(myCandidateMatcher);
			}
			JavaPairRDD<IntArray, Integer> reducedTransactions = transactionsMapped.reduceByKey(reducer);
			JavaPairRDD<IntArray, Integer> filteredSupport = reducedTransactions.filter(minSupportFilter);
			List<Tuple2<IntArray, Integer>> collected = filteredSupport.collect();
			System.out.println("the map-reduce-step took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");

			startTime = System.currentTimeMillis();
			candidates = generateCandidatesInt(collected);
			System.out.println("the candidate generation took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds and generated " + candidates.size() + " candidates");
		} while (candidates.size() > 0);
	}

	private static void aprioriOnStrings(String[] args, JavaSparkContext context) {
		Function<String, List<String>> transactionParser = new Function<String, List<String>>() {
			private static final long serialVersionUID = -4625524329716723997L;

			public List<String> call(String line) throws Exception {
				String[] items = line.split(" ");
				Arrays.sort(items);
				return Arrays.asList(items);
			}

		};

		PairFlatMapFunction<List<String>, List<String>, Integer> singleItemsExtractor = new PairFlatMapFunction<List<String>, List<String>, Integer>() {
			private static final long serialVersionUID = -6501622718277608505L;

			public Iterable<Tuple2<List<String>, Integer>> call(List<String> transaction) throws Exception {
				HashSet<Tuple2<List<String>, Integer>> result = new HashSet<Tuple2<List<String>, Integer>>();

				for (String item : transaction) {
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

		boolean firstRound = true;
		Set<List<String>> candidates = null;
		long startTime = System.currentTimeMillis();
		JavaRDD<String> inputLines = context.textFile(args[0]);
		JavaRDD<List<String>> transactions = inputLines.map(transactionParser);

		do {
			JavaPairRDD<List<String>, Integer> transactionsMapped;
			if (firstRound) {
				firstRound = false;
				transactionsMapped = transactions.flatMapToPair(singleItemsExtractor);
			} else {
				CandidateMatcher myCandidateMatcher = new CandidateMatcher(candidates);
				transactionsMapped = transactions.flatMapToPair(myCandidateMatcher);
			}
			JavaPairRDD<List<String>, Integer> reducedTransactions = transactionsMapped.reduceByKey(reducer);
			JavaPairRDD<List<String>, Integer> filteredSupport = reducedTransactions.filter(minSupportFilter);
			List<Tuple2<List<String>, Integer>> collected = filteredSupport.collect();
			System.out.println("the map-reduce-step took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");

			startTime = System.currentTimeMillis();
			candidates = generateCandidates(collected);
			System.out.println("the candidate generation took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds and generated " + candidates.size() + " candidates");
		} while (candidates.size() > 0);
	}

	public static void main(String[] args) throws IOException {

		if (args.length < 2) {
			System.out.println("please provide the following parameters: input_path and result_file_path");
			System.exit(0);
		}

		SparkConf sparkConf = new SparkConf().setAppName(Main.class.getName()).set("spark.hadoop.validateOutputSpecs", "false");
		JavaSparkContext context = new JavaSparkContext(sparkConf);

		// aprioriOnStrings(args, context);
		aprioriOnInts(args, context);

		context.close();
	}
}