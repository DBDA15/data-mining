package de.hpi.dbda;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import de.hpi.dbda.trie.InnerTrieNode;

public class Main {
	
	
	public static Map<Integer, String> compressionMapping = new HashMap<Integer, String>();
	public static Set largeItems = new HashSet();
	public static HashMap<Set,Integer> allSupport = new HashMap<Set, Integer>();
	public static double minConf;
	public static int minSupport;
	public static int minRating;
	
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
	
	public static Set<List<String>> generateCandidates(
			List<Tuple2<List<String>, Integer>> largeItemTuples) {
		
		Set<List<String>> largeItems = new HashSet<List<String>>(largeItemTuples.size());
		for (Tuple2<List<String>, Integer> tuple : largeItemTuples) {
			largeItems.add(tuple.f0);
			System.out.print(tuple.f0);
			System.out.print(", ");
		}

		HashSet<List<String>> result = new HashSet<List<String>>();
		MultiValueMapWithArrayList map = new MultiValueMapWithArrayList();
		
		for (int i = 0; i < largeItemTuples.size(); i++) {
			ListPointer key = new ListPointer(
					largeItemTuples.get(i).f0, largeItemTuples.get(i).f0.size() - 1);
			
			map.put(key, largeItemTuples.get(i).f0);
		}

		for (Object key : map.keySet()) {
			ArrayList<List<String>> entrySet = (ArrayList<List<String>>) map.getCollection(key);
			for (int i = 0; i < entrySet.size() - 1; i++) {
				for (int j = i + 1; j < entrySet.size(); j++) {
					List<String> candidate;
					if (entrySet.get(i).get(entrySet.get(i).size() - 1)
							.compareTo(entrySet.get(j)
							.get(entrySet.get(j).size() - 1)) < 0) {
								// if the item set at position i is "smaller than" the item set at position j
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
			largeItems.add(tuple.f0);
		}

		HashSet<IntArray> result = new HashSet<IntArray>();
		MultiValueMapWithArrayList map = new MultiValueMapWithArrayList();
		for (int i = 0; i < largeItemTuples.size(); i++) {
			IntArrayPointer key = new IntArrayPointer(largeItemTuples.get(i).f0, largeItemTuples.get(i).f0.value.length - 1);
			map.put(key, largeItemTuples.get(i).f0);
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

	private static void aprioriOnIntsWithTrie(String[] args, ExecutionEnvironment env) throws Exception {
		MapFunction<String, IntArray> transactionParser = new MapFunction<String, IntArray>() {
			private static final long serialVersionUID = -2163238643793472047L;

			public IntArray map(String line) {
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

		FlatMapFunction<IntArray, Tuple2<Integer, Integer>> singleItemsExtractor = new FlatMapFunction<IntArray, Tuple2<Integer, Integer>>() {
			private static final long serialVersionUID = 4206575656443369070L;

			public void flatMap(IntArray transaction, Collector<Tuple2<Integer, Integer>> out) throws Exception {
				for (int item : transaction.value) {
					out.collect(new Tuple2<Integer, Integer>(item, 1));
				}
			}

		};

		GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> reducer = new Reducer<Integer>();

		FilterFunction<Tuple2<Integer, Integer>> minSupportFilter = new MinSupportFilter<Integer>(minSupport);

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
		DataSet<String> inputLines = env.readTextFile(args[0]);
		DataSet<IntArray> transactions = inputLines.map(transactionParser);
		do {
			DataSet<Tuple2<Integer, Integer>> transactionsMapped;
			if (firstRound) {
				transactionsMapped = transactions.flatMap(singleItemsExtractor);
			} else {
				CandidateMatcherTrie myCandidateMatcher = new CandidateMatcherTrie(trie);
				transactionsMapped = transactions.flatMap(myCandidateMatcher);
			}
			DataSet<Tuple2<Integer, Integer>> reducedTransactions = transactionsMapped.groupBy(0).reduceGroup(reducer);
			
			DataSet<Tuple2<Integer, Integer>> filteredSupport = reducedTransactions.filter(minSupportFilter);
			List<Tuple2<Integer, Integer>> collected = filteredSupport.collect();

			//count confidence
			List<Tuple2<IntArray, Integer>> collectedItemSets = spellOutLargeItems(collected, firstRound);
			for (Tuple2<IntArray, Integer> tuple : collectedItemSets) {
				allSupport.put(tuple.f0.valueSet(), tuple.f1);
			}
			
			System.out.println("the map-reduce-step took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");

			startTime = System.currentTimeMillis();
			List<Tuple2<IntArray, Integer>> largeItemSets= spellOutLargeItems(collected, firstRound);
			candidates = generateCandidatesInt(largeItemSets);
			largeItems.addAll(candidates);
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
			System.out.println("the candidate generation took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds and generated " + candidates.size() + " candidates");
		} while (candidates.size() > 0);
	}

	public static List<Tuple2<IntArray, Integer>> spellOutLargeItems(List<Tuple2<Integer, Integer>> collected, boolean firstRound) {
		ArrayList<Tuple2<IntArray, Integer>> result = new ArrayList<Tuple2<IntArray, Integer>>(collected.size());
		if (firstRound) {
			for (Tuple2<Integer, Integer> largeItemSet : collected) {
				result.add(new Tuple2<IntArray, Integer>(new IntArray(new int[] { largeItemSet.f0 }), largeItemSet.f1));
			}
		} else {
			for (Tuple2<Integer, Integer> largeItemSet : collected) {
				result.add(new Tuple2<IntArray, Integer>(candidateLookup.get(largeItemSet.f0), largeItemSet.f1));
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

	private static void aprioriOnInts(String[] args, ExecutionEnvironment env) throws Exception {
		MapFunction<String, IntArray> transactionParser = new MapFunction<String, IntArray>() {
			private static final long serialVersionUID = -2163238643793472047L;

			public IntArray map(String line) {
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

		FlatMapFunction<IntArray, Tuple2<IntArray, Integer>> singleItemsExtractor = new FlatMapFunction<IntArray, Tuple2<IntArray, Integer>>() {
			private static final long serialVersionUID = 4206575656443369070L;

			public void flatMap(IntArray transaction, Collector<Tuple2<IntArray, Integer>> out) {
				for (int item : transaction.value) {
					IntArray itemList = new IntArray(new int[] { item });
					out.collect(new Tuple2<IntArray, Integer>(itemList, 1));
				}
			}

		};

		GroupReduceFunction<Tuple2<IntArray, Integer>, Tuple2<IntArray, Integer>> reducer = new Reducer<IntArray>();			
		FilterFunction<Tuple2<IntArray, Integer>> minSupportFilter = new MinSupportFilter<IntArray>(Main.minSupport);

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
		DataSet<String> inputLines = env.readTextFile(args[0]);
		DataSet<IntArray> transactions = inputLines.map(transactionParser);

		do {
			DataSet<Tuple2<IntArray, Integer>> transactionsMapped;
			if (firstRound) {
				firstRound = false;
				transactionsMapped = transactions.flatMap(singleItemsExtractor);
			} else {
				CandidatesListMatcher myCandidateMatcher = new CandidatesListMatcher(candidates);
				transactionsMapped = transactions.flatMap(myCandidateMatcher);
			}
			DataSet<Tuple2<IntArray, Integer>> reducedTransactions = transactionsMapped.groupBy(new KeySelector<Tuple2<IntArray,Integer>,String>(){
				private static final long serialVersionUID = 3029831776513575439L;

				public String getKey(Tuple2<IntArray, Integer> t) {
					String result = "";
					for (int i : t.f0.value) {
						result += Integer.toString(i) + " ";
					}
					return result;
				}
			}).reduceGroup(reducer);
			DataSet<Tuple2<IntArray, Integer>> filteredSupport = reducedTransactions.filter(minSupportFilter);
			List<Tuple2<IntArray, Integer>> collected = filteredSupport.collect();

			for (Tuple2<IntArray, Integer> tuple : collected) {
				allSupport.put(tuple.f0.valueSet(), tuple.f1);
			}
			
			System.out.println("the map-reduce-step took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");

			startTime = System.currentTimeMillis();
			candidates = generateCandidatesInt(collected);
			largeItems.addAll(candidates);
			System.out.println("the candidate generation took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds and generated " + candidates.size() + " candidates");
		} while (candidates.size() > 0);
	}
	
	private static void aprioriOnStrings(String[] args, ExecutionEnvironment env) throws Exception {
		
		MapFunction<String, List<String>> transactionParser = new MapFunction<String, List<String>>() {
			private static final long serialVersionUID = -4625524329716723997L;

			public List<String> map(String line) {
				String[] items = line.split(" ");
				if (items.length > 0) {
					Arrays.sort(items);
					return Arrays.asList(items);
				} else {
					return new ArrayList<String>(1);
				}
			}

		};

		FlatMapFunction<List<String>, Tuple2<List<String>, Integer>> singleItemsExtractor = 
				new FlatMapFunction<List<String>, Tuple2<List<String>, Integer>>() {
			private static final long serialVersionUID = -6501622718277608505L;

			public void flatMap(List<String> transaction, Collector<Tuple2<List<String>,Integer>> out) {

				for (String item : transaction) {
					List<String> itemList = new ArrayList<String>();
					itemList.add(item);
					out.collect(new Tuple2<List<String>, Integer>(itemList, 1));
				}
			}

		};

		GroupReduceFunction<Tuple2<List<String>, Integer>, Tuple2<List<String>, Integer>> reducer = new Reducer<List<String>>();

		FilterFunction<Tuple2<List<String>, Integer>> minSupportFilter = 
				new MinSupportFilter<List<String>>(Main.minSupport);

		boolean firstRound = true;
		Set<List<String>> candidates = null;
		long startTime = System.currentTimeMillis();
		DataSet<String> inputLines = env.readTextFile(args[0]);
		DataSet<List<String>> transactions = inputLines.map(transactionParser);

		do {
			DataSet<Tuple2<List<String>, Integer>> transactionsMapped;
			if (firstRound) {
				firstRound = false;
				transactionsMapped = transactions.flatMap(singleItemsExtractor);
			} else {
				CandidateMatcher myCandidateMatcher = new CandidateMatcher(candidates);
				transactionsMapped = transactions.flatMap(myCandidateMatcher);
			}
			DataSet<Tuple2<List<String>, Integer>> reducedTransactions = transactionsMapped.groupBy(
					new KeySelector<Tuple2<List<String>,Integer>,String>(){
						private static final long serialVersionUID = 127074927072828508L;

						public String getKey(Tuple2<List<String>,Integer> t) {
							String result = StringUtils.join(t.f0, " ");
							return result;
						}
					}).reduceGroup(reducer);
			DataSet<Tuple2<List<String>, Integer>> filteredSupport = 
					reducedTransactions.filter(minSupportFilter);
			List<Tuple2<List<String>, Integer>> collected = filteredSupport.collect();

			for (Tuple2<List<String>, Integer> tuple : collected) {
				allSupport.put(new HashSet<String>(tuple.f0), tuple.f1);
			}

			System.out.println("the map-reduce-step took " + 
					((System.currentTimeMillis() - startTime) / 1000) + " seconds");

			startTime = System.currentTimeMillis();
			candidates = generateCandidates(collected);
			largeItems.addAll(candidates);
			System.out.println("the candidate generation took " + 
					((System.currentTimeMillis() - startTime) / 1000) + 
					" seconds and generated " + candidates.size() + 
					" candidates");
		} while (candidates.size() > 0);
	}

	public static void main(String[] args) throws Exception {

        if (args.length < 6) {
            System.out.println("please provide the following parameters: input_path, " +
            		"result_file_path, modus, minSupport, minRating and minConfidence");
            System.out.println("for example:");
            System.out.println("./spark-submit --master spark://172.16.21.111:7077 " +
            		"--driver-memory 7g --conf spark.executor.memory=4g " +
            		"--class de.hpi.dbda.Main " +
            		"/home/martin.gebert/Apriori_Data_Mining-0.0.1-SNAPSHOT.jar " +
            		"hdfs://tenemhead2:8020/data/netflix.txt " +
            		"/home/martin.gebert/result.txt ints 8000 5 0.5");
            System.exit(0);
        }
        
		minSupport = Integer.parseInt(args[3]);
		minRating = Integer.parseInt(args[4]);
		minConf = Double.parseDouble(args[5]);

		final ExecutionEnvironment env = ExecutionEnvironment
		        .getExecutionEnvironment();

        switch(args[2]) {
            case "strings":
            	aprioriOnStrings(args, env);
            break;
            case "ints":
            aprioriOnInts(args, env);
            break;
            case "trie":
            aprioriOnIntsWithTrie(args, env);
            break;
            default:
            System.out.println("unknown mode");
            break;
        }
       
        ArrayList<AssociationRule> ar = new ArrayList<AssociationRule>();
        //CHANGE IF FPC - IntArray candidate : FPC.largeItemss
        //for (IntArray candidate : largeItems) {

		if (largeItems.size() > 0 && largeItems.iterator().next() instanceof IntArray) {
			for (IntArray candidate : (Set<IntArray>) largeItems) {
				HashSet<HashSet<Integer>> pow = Utilities.powerSet(candidate);
				pow.remove(new HashSet<Integer>());
				pow.remove(candidate.valueSet());

				for (HashSet<Integer> s : pow) {
					HashSet<Integer> o = candidate.valueSet();
					o.removeAll(s);
					AssociationRule<Integer> asr = new AssociationRule<Integer>(s, o);
					if (checkARConfidenceAndSupport(asr)) {
						System.out.println(Utilities.printAssociationRule(asr));
						ar.add(asr);
					}
				}
			}
		}
		else if (largeItems.size() > 0 && largeItems.iterator().next() instanceof List<?>) {
			for (List<String> candidate : (Set<List<String>>) largeItems) {
				HashSet<HashSet<String>> pow = Utilities.powerSet(candidate);
				pow.remove(new HashSet<String>());
				pow.remove(new HashSet<String>(candidate));

				for (HashSet<String> s : pow) {
					HashSet<String> o = new HashSet<String>(candidate);
					o.removeAll(s);
					AssociationRule<String> asr = new AssociationRule<String>(s, o);
					if (checkARConfidenceAndSupport(asr)) {
						System.out.println(Utilities.printAssociationRule(asr));
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
            bw.write(Utilities.printAssociationRule(arule));
        }
        bw.close();

    }
}