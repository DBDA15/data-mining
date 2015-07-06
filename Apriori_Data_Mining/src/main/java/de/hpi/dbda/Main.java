package de.hpi.dbda;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;

import de.hpi.dbda.trie.InnerTrieNode;
import de.hpi.dbda.test;
import de.hpi.dbda.FPC;
import scala.Tuple2;


public class Main {
	
	public static Map<Integer, String> compressionMapping = new HashMap<Integer, String>();
	public static Set largeItems = new HashSet();
	public static HashMap<Set,Integer> allSupport = new HashMap<Set, Integer>();
	public static double minConf;
	public static int minSupport;

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
	
	public static boolean checkARConfidenceAndSupport(AssociationRule ar) {
		HashSet f = ar.first;
		HashSet l = ar.last;
		HashSet a = (HashSet) f.clone();
		a.addAll(l);
		double confidence = 0;
		if (!allSupport.containsKey(a)) {

		} else if (!allSupport.containsKey(f)) {
		} else {
			confidence = (double) allSupport.get(a) / (double) allSupport.get(f);
			if (allSupport.get(a) < minSupport) {
				confidence = 0;
			}
		}
		
		return confidence > minConf;
	}
	
	public static String printAssociationRule(AssociationRule ar){
		String al = "";
		Set f = ar.first;
		Set l = ar.last;
		String itemString;
		for (Object i : f) {
			itemString = (String) (i instanceof String ? i : i.toString());
			al += itemString + ", ";
		}
		al += " => ";
		for (Object i : l) {
			itemString = (String) (i instanceof String ? i : i.toString());
			al += itemString + ", ";
		}
		al += "\n";
		return al;
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
				}
			}
		}

		return result;
	}

	private static ArrayList<IntArray> candidateLookup = null;

	public static InnerTrieNode candidatesToTrie(Set<IntArray> candidates) {
		
		TreeSet<IntArray> sortedCandidates = new TreeSet<IntArray>(candidates);
		//TODO length +2 hard coded, may be fixed with DPC
		InnerTrieNode[] currentTriePath = new InnerTrieNode[sortedCandidates.iterator().next().value.length+3];
		candidateLookup = new ArrayList<IntArray>(sortedCandidates);

		// for every candidate, find the smallest index at which it differs from the previous candidate
		int[] firstDifferentElementIndices = new int[sortedCandidates.size()];
		firstDifferentElementIndices[0] = sortedCandidates.iterator().next().value.length - 1;
		IntArray previousCandidate = null;
		int candidateIndex = 0;
		for (IntArray candidate : sortedCandidates) {
			if (previousCandidate != null) {
				//TODO check if it is right
				firstDifferentElementIndices[candidateIndex] = Math.min(candidate.value.length, 
																		previousCandidate.value.length);
				for (int i = 0; i < Math.min(candidate.value.length, previousCandidate.value.length); i++) {
					if (candidate.value[i] != previousCandidate.value[i]) {
						firstDifferentElementIndices[candidateIndex] = i;
						break;
					}
				}
			}
			candidateIndex++;
			previousCandidate = candidate;
		}
		
		// create a trie containing the nodes for the first candidate only
		for (int level = 0; level < sortedCandidates.iterator().next().value.length; level++) {
			int childCount = countChildren(firstDifferentElementIndices, 0, level);

			InnerTrieNode newNode = new InnerTrieNode(new int[childCount], new InnerTrieNode[childCount]);
			if (level != 0) {
				currentTriePath[level - 1].edgeLabels[0] = sortedCandidates.iterator().next().value[level - 1];
				currentTriePath[level - 1].children[0] = newNode;
			}
			currentTriePath[level] = newNode;
		}

		candidateIndex = 0;
		for (IntArray candidate : sortedCandidates) {
			// find the leftmost child slot that doesn't contain any data
			int childIndex;
			int entryLevel = firstDifferentElementIndices[candidateIndex] + 1; //Entry level for for-loop if anchorNode == null
			InnerTrieNode anchorNode = currentTriePath[firstDifferentElementIndices[candidateIndex]];
			if (anchorNode != null) {
				for (childIndex = anchorNode.children.length - 1; 
					 anchorNode.children[childIndex] == null && childIndex > 0;
					 childIndex--) {
					// nothing to do here - the whole logic is in the loop header
				}
				if (anchorNode.children[childIndex] != null) {
					childIndex++;
				}
			} else {
				childIndex = 0;
				entryLevel -= 1;
			}
					

			// create the new InnerTrieNodes that are needed for candidate
			for (int level = entryLevel; level < candidate.value.length; level++) {
				int childCount = countChildren(firstDifferentElementIndices, candidateIndex, level);

				InnerTrieNode newNode = new InnerTrieNode(new int[childCount], new InnerTrieNode[childCount]);
				currentTriePath[level - 1].edgeLabels[childIndex] = candidate.value[level - 1];
				currentTriePath[level - 1].children[childIndex] = newNode;
				currentTriePath[level] = newNode;
				childIndex = 0;
			}

			// create the TrieLeaf for candidate
			int level = candidate.value.length;
			int newNodeIndex = -1;
			for (int edge = 0; edge < currentTriePath[level-1].edgeLabels.length; edge++) {
				if (currentTriePath[level-1].edgeLabels[edge] == candidate.value[level - 1]) {
					newNodeIndex = edge;
					break;
				}
			}
			if (newNodeIndex == -1) {
				int childrenCount =  countChildren(firstDifferentElementIndices,candidateIndex, candidate.length());
				InnerTrieNode newNode = new InnerTrieNode(candidateIndex, new int[childrenCount], new InnerTrieNode[childrenCount]);
				currentTriePath[level]=newNode;
				currentTriePath[level - 1].edgeLabels[childIndex] = candidate.value[level - 1];
				currentTriePath[level - 1].children[childIndex] = newNode;
			} else {
				currentTriePath[level - 1].children[newNodeIndex].candidateID = candidateIndex;
			}
			candidateIndex++;
		}
		
		return currentTriePath[0];
	}

	private static int countChildren(int[] firstDifferentElementIndices, int currentCandidateIndex, int level) {
		int childCount = 1;
		for (int i = currentCandidateIndex + 1; i < firstDifferentElementIndices.length; i++) {
			if (firstDifferentElementIndices[i] == level) {
				if (candidateLookup.get(i).value.length > level) {
					childCount++;
				}
			} else if (firstDifferentElementIndices[i] < level) {
				break;
			}
		}
		return childCount;
	}

	//TODO was private
	public static List<IntArray> intCompressInputFile(String inputPath) throws IOException {
		final int LINES_PER_PRINT = 10000;
		
		Map<String, Integer> compressionMap = new HashMap<String, Integer>();
		
		int lineCounter = 0;
		InputStreamReader myInputStream;
		FileSystem fs = null;
		Path pt = null;
		if (inputPath.startsWith("hdfs")) {
			pt = new Path(inputPath);
			Configuration conf = new Configuration();
			conf.set("fs.default.name", "hdfs://tenemhead2:8020");
			conf.addResource(pt);
			fs = FileSystem.get(conf);
			myInputStream = new InputStreamReader(fs.open(pt));
		} else {
			myInputStream = new FileReader(inputPath);
		}

		BufferedReader reader = new BufferedReader(myInputStream);
		String line;
		String[] words;
		int compressionOutput = Integer.MIN_VALUE;
		while ((line = reader.readLine()) != null) {
			words = line.split(" ");
			if (words.length > 0) {
				for (String myWord : words) {
					compressionMap.put(myWord, compressionOutput);
					compressionMapping.put(compressionOutput, myWord);
					compressionOutput++;
				}
			}
			
			lineCounter++;
			if (lineCounter % LINES_PER_PRINT == 0) {
				System.out.println("read " + lineCounter + " lines (1st read)");
			}
		}
		reader.close();
		
		lineCounter = 0;
		if (inputPath.startsWith("hdfs")) {
			myInputStream = new InputStreamReader(fs.open(pt));
		} else {
			myInputStream = new FileReader(inputPath);
		}

		reader = new BufferedReader(myInputStream);
		ArrayList<IntArray> compressedFile = new ArrayList<IntArray>();
		int[] compressedLine;
		while ((line = reader.readLine()) != null) {
			words = line.split(" ");
			if (words.length > 0) {
				compressedLine = new int[words.length];
				for (int i = 0; i < words.length; i++) {
					compressionOutput = compressionMap.get(words[i]);
					compressedLine[i] = compressionOutput;
				}
				compressedFile.add(new IntArray(compressedLine));
			}

			lineCounter++;
			if (lineCounter % LINES_PER_PRINT == 0) {
				System.out.println("read " + lineCounter + " lines (2nd read)");
			}
		}
		reader.close();
		return compressedFile;
	}

	private static void aprioriOnIntsWithTrie(String[] args, JavaSparkContext context) throws IOException {
		Function<String, IntArray> transactionParser = new Function<String, IntArray>() {

			private static final long serialVersionUID = -7073931999435470322L;

			public IntArray call(String line) throws Exception {
				String[] items = line.split(" ");
				int[] itemset = new int[items.length];
				for (int i = 0; i < items.length; i++){
					itemset[i] = Integer.parseInt(items[i]);
				}
				Arrays.sort(itemset);
				IntArray ar = new IntArray(itemset);
				return ar;
			}

		};

		PairFlatMapFunction<IntArray, Integer, Integer> singleItemsExtractor = new PairFlatMapFunction<IntArray, Integer, Integer>() {
			private static final long serialVersionUID = -2714321550777030577L;

			public Iterable<Tuple2<Integer, Integer>> call(IntArray transaction) throws Exception {
				HashSet<Tuple2<Integer, Integer>> result = new HashSet<Tuple2<Integer, Integer>>();

				for (int item : transaction.value) {
					result.add(new Tuple2<Integer, Integer>(item, 1));
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

		Function<Tuple2<Integer, Integer>, Boolean> minSupportFilter = new MinSupportFilter<Integer>(minSupport);

		boolean firstRound = true;
		Set<IntArray> candidates = null;
		InnerTrieNode trie = null;
		/*
		long startTime = System.currentTimeMillis();
		List<IntArray> compressedInputFile = intCompressInputFile(args[0]);
		System.out.println("compressing the input file on the master took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
		System.out.println("Memory in use [MB]: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024);

		startTime = System.currentTimeMillis();
		JavaRDD<IntArray> inputLines = context.parallelize(compressedInputFile);
		System.out.println("parallelizing the compressed input file took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
*/
		long startTime = System.currentTimeMillis();
		JavaRDD<String> inputLines = context.textFile(args[0]);
		JavaRDD<IntArray> transactions = inputLines.map(transactionParser);
		transactions.persist(StorageLevel.MEMORY_ONLY());
		do {
			JavaPairRDD<Integer, Integer> transactionsMapped;
			if (firstRound) {
				transactionsMapped = transactions.flatMapToPair(singleItemsExtractor);
			} else {
				CandidateMatcherTrie myCandidateMatcher = new CandidateMatcherTrie(trie);
				transactionsMapped = transactions.flatMapToPair(myCandidateMatcher);
			}
			JavaPairRDD<Integer, Integer> reducedTransactions = transactionsMapped.reduceByKey(reducer);
			
			JavaPairRDD<Integer, Integer> filteredSupport = reducedTransactions.filter(minSupportFilter);
			List<Tuple2<Integer, Integer>> collected = filteredSupport.collect();

			//count confidence
			List<Tuple2<IntArray, Integer>> collectedItemSets = spellOutLargeItems(collected, firstRound);
			for (Tuple2<IntArray, Integer> tuple : collectedItemSets) {
				allSupport.put(tuple._1.valueSet(), tuple._2);
			}
			
			System.out.println("finished another map-reduce-step");

			List<Tuple2<IntArray, Integer>> largeItemSets= spellOutLargeItems(collected, firstRound);
			candidates = generateCandidatesInt(largeItemSets);
			if (candidates.size() > 0) {
				trie = candidatesToTrie(candidates);
			}
			firstRound = false;
			
			/*for (IntArray i : candidates) {
				for (int j = 0; j < i.length(); j++) {
					System.out.print(compressionMapping.get(i.value[j]));
					System.out.print(" ");
				}
				System.out.print("; ");
			}
			System.out.println("");*/
		} while (candidates.size() > 0);
		
		System.out.println("runtime: " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
		System.out.println("large item sets: " + allSupport.size()); 
		}

	//TODO was private
	public static List<Tuple2<IntArray, Integer>> spellOutLargeItems(List<Tuple2<Integer, Integer>> collected, boolean firstRound) {
		ArrayList<Tuple2<IntArray, Integer>> result = new ArrayList<Tuple2<IntArray, Integer>>(collected.size());
		if (firstRound) {
			for (Tuple2<Integer, Integer> largeItemSet : collected) {
				result.add(new Tuple2<IntArray, Integer>(new IntArray(new int[] { largeItemSet._1 }), largeItemSet._2));
			}
		} else {
			for (Tuple2<Integer, Integer> largeItemSet : collected) {
				result.add(new Tuple2<IntArray, Integer>(candidateLookup.get(largeItemSet._1), largeItemSet._2));
			}
		}
		return result;
	}

	public static void printTrie(InnerTrieNode node) {
		if (node == null) {
			System.out.println("null");
			return;
		}
		if (node.candidateID != -1) {
			System.out.println("<" + candidateLookup.get((node).candidateID).printDecoded(compressionMapping) + ">");
		} else {
			InnerTrieNode innerNode = (InnerTrieNode) node;
			System.out.println();
			for (int i = 0; i < innerNode.edgeLabels.length; i++) {
				System.out.print(compressionMapping.get(innerNode.edgeLabels[i]) + ", ");
			}
			for (int i = 0; i < innerNode.edgeLabels.length; i++) {
				printTrie(innerNode.children[i]);
			}
			System.out.println();
		}
	}

	private static void aprioriOnInts(String[] args, JavaSparkContext context) throws IOException {
		Function<String, IntArray> transactionParser = new Function<String, IntArray>() {

			private static final long serialVersionUID = -2526089249063746690L;

			public IntArray call(String line) throws Exception {
				String[] items = line.split(" ");
				int[] itemset = new int[items.length];
				for (int i = 0; i < items.length; i++){
					itemset[i] = Integer.parseInt(items[i]);
				}
				Arrays.sort(itemset);
				IntArray ar = new IntArray(itemset);
				return ar;
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

		Function<Tuple2<IntArray, Integer>, Boolean> minSupportFilter = new MinSupportFilter<IntArray>(Main.minSupport);

		boolean firstRound = true;
		Set<IntArray> candidates = null;

		long startTime = System.currentTimeMillis();
		/*List<IntArray> compressedInputFile = intCompressInputFile(args[0]);
		System.out.println("compressing the input file on the master took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
		System.out.println("Memory in use [MB]: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024);

		startTime = System.currentTimeMillis();
		JavaRDD<IntArray> inputLines = context.parallelize(compressedInputFile);
		System.out.println("parallelizing the compressed input file took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");

		startTime = System.currentTimeMillis();*/
		JavaRDD<String> inputLines = context.textFile(args[0]);
		JavaRDD<IntArray> transactions = inputLines.map(transactionParser);

		do {
			JavaPairRDD<IntArray, Integer> transactionsMapped;
			if (firstRound) {
				firstRound = false;
				transactionsMapped = transactions.flatMapToPair(singleItemsExtractor);
			} else {
				CandidatesListMatcher myCandidateMatcher = new CandidatesListMatcher(candidates);
				transactionsMapped = transactions.flatMapToPair(myCandidateMatcher);
			}
			JavaPairRDD<IntArray, Integer> reducedTransactions = transactionsMapped.reduceByKey(reducer);
			JavaPairRDD<IntArray, Integer> filteredSupport = reducedTransactions.filter(minSupportFilter);
			List<Tuple2<IntArray, Integer>> collected = filteredSupport.collect();

			for (Tuple2<IntArray, Integer> tuple : collected) {
				allSupport.put(tuple._1.valueSet(), tuple._2);
			}
			
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
				if (items.length > 0) {
					Arrays.sort(items);
					return Arrays.asList(items);
				} else {
					return new ArrayList<String>(1);
				}
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

		Function<Tuple2<List<String>, Integer>, Boolean> minSupportFilter = new MinSupportFilter<List<String>>(Main.minSupport);

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

			for (Tuple2<List<String>, Integer> tuple : collected) {
				allSupport.put(new HashSet<String>(tuple._1), tuple._2);
			}

			System.out.println("the map-reduce-step took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");

			startTime = System.currentTimeMillis();
			candidates = generateCandidates(collected);
			largeItems.addAll(candidates);
			System.out.println("the candidate generation took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds and generated " + candidates.size() + " candidates");
		} while (candidates.size() > 0);
	}

	public static void main(String[] args) throws IOException {

        if (args.length < 5) {
            System.out.println("please provide the following parameters: input_path, result_file_path, modus, minSupport, minRating and minConfidence");
            System.out.println("for example:");
            System.out.println("./spark-submit --master spark://172.16.21.111:7077 --driver-memory 7g --conf spark.executor.memory=4g --class de.hpi.dbda.Main /home/martin.gebert/Apriori_Data_Mining-0.0.1-SNAPSHOT.jar hdfs://tenemhead2:8020/data/netflix.txt /home/martin.gebert/result.txt ints 8000 5 0.5");
            System.exit(0);
        }
        
		minSupport = Integer.parseInt(args[3]);
		minConf = Double.parseDouble(args[4]);

        SparkConf sparkConf = new SparkConf().setAppName(Main.class.getName()).set("spark.hadoop.validateOutputSpecs", "false");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        switch(args[2]) {
            case "strings":
            aprioriOnStrings(args, context);
            break;
            case "ints":
            aprioriOnInts(args, context);
            break;
            case "trie":
            aprioriOnIntsWithTrie(args, context);
            break;
            case "fpc":
            FPC.fpcOnIntsWithTrie(args, context);
            Main.allSupport = FPC.allSupport;
            break;
            default:
            System.out.println("unknown mode");
            break;
        }
       
        ArrayList<AssociationRule> ar = new ArrayList<AssociationRule>();
        //CHANGE IF FPC - IntArray candidate : FPC.largeItemss
        //for (IntArray candidate : largeItems) {

		if (allSupport.size() > 0 && allSupport.keySet().iterator().next().iterator().next() instanceof Integer) {
			for (Set<Integer> candidate : allSupport.keySet()) {
				HashSet<HashSet<Integer>> pow = test.powerSet(new ArrayList<Integer>(candidate));
				pow.remove(new HashSet<Integer>());
				pow.remove(candidate);

				for (HashSet<Integer> s : pow) {
					HashSet<Integer> o = new HashSet<Integer>(candidate);
					o.removeAll(s);
					AssociationRule<Integer> asr = new AssociationRule<Integer>(s, o);
					if (checkARConfidenceAndSupport(asr)) {
						System.out.println(printAssociationRule(asr));
						ar.add(asr);
					}
				}
			}
		}
		else if (largeItems.size() > 0 && largeItems.iterator().next() instanceof List<?>) {
			for (List<String> candidate : (Set<List<String>>) largeItems) {
				HashSet<HashSet<String>> pow = test.powerSet(candidate);
				pow.remove(new HashSet<String>());
				pow.remove(new HashSet<String>(candidate));

				for (HashSet<String> s : pow) {
					HashSet<String> o = new HashSet<String>(candidate);
					o.removeAll(s);
					AssociationRule<String> asr = new AssociationRule<String>(s, o);
					if (checkARConfidenceAndSupport(asr)) {
						System.out.println(printAssociationRule(asr));
						ar.add(asr);
					}
				}
			}
		}
        
        File file = new File(args[1]);
         
        // if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        for (AssociationRule arule : ar) {
            bw.write(printAssociationRule(arule));
        }
        bw.close();
       
        context.close();
    }
}