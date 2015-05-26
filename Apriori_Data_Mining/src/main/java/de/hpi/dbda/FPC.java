package de.hpi.dbda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import de.hpi.dbda.trie.InnerTrieNode;
import de.hpi.dbda.Main;

public class FPC {
	
	public static Set<IntArray> largeItemss = new HashSet<IntArray>();
	public static HashMap<Set<Integer>,Integer> allSupport = new HashMap<Set<Integer>, Integer>();

	public static Set<IntArray> generateCandidatesInt(Set<IntArray> largeItems) {

		HashSet<IntArray> result = new HashSet<IntArray>();
		MultiValueMapWithArrayList map = new MultiValueMapWithArrayList();
		for (IntArray li : largeItems) {
			IntArrayPointer key = new IntArrayPointer(li, li.value.length - 1);
			map.put(key, li);
		}

		for (Object key : map.keySet()) {
			ArrayList<IntArray> entrySet = (ArrayList<IntArray>) map.getCollection(key);
			for (int i = 0; i < entrySet.size() - 1; i++) {
				for (int j = i + 1; j < entrySet.size(); j++) {
					IntArray candidate;
					// if the item set at position i is "smaller than" the item set at position j
					if (entrySet.get(i).value[entrySet.get(i).value.length - 1] < 
							entrySet.get(j).value[entrySet.get(j).value.length - 1]) { 
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
	
	public static void fpcOnIntsWithTrie(String[] args, JavaSparkContext context) throws IOException {
		Function<IntArray, IntArray> transactionParser = new Function<IntArray, IntArray>() {
			private static final long serialVersionUID = 165521644289765913L;

			public IntArray call(IntArray items) throws Exception {
				Arrays.sort(items.value);
				return items;
			}

		};

		PairFlatMapFunction<IntArray, Integer, Integer> singleItemsExtractor = 
				new PairFlatMapFunction<IntArray, Integer, Integer>() {
			
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

		Function<Tuple2<Integer, Integer>, Boolean> minSupportFilter = new Function<Tuple2<Integer, Integer>, Boolean>() {
			private static final long serialVersionUID = 1188423613305352530L;
			private static final int minSupport = 1000;

			public Boolean call(Tuple2<Integer, Integer> input) throws Exception {
				if (input._2 >= minSupport) {
					System.out.println(input._2);
				}
				return input._2 >= minSupport;
			}
		};
		
		

		boolean firstRound = true;
		boolean secondRound = true;
		Set<IntArray> candidates = null;
		InnerTrieNode trie = null;

		long startTime = System.currentTimeMillis();
		List<IntArray> compressedInputFile = Main.intCompressInputFile(args[0]);
		
		System.out.println("compressing the input file on the master took " +
						  ((System.currentTimeMillis() - startTime) / 1000) + 
						  " seconds");
		System.out.println("Memory in use [MB]: " + 
						  (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024);

		startTime = System.currentTimeMillis();
		JavaRDD<IntArray> inputLines = context.parallelize(compressedInputFile);
		System.out.println("parallelizing the compressed input file took " + 
						  ((System.currentTimeMillis() - startTime) / 1000) + 
						  " seconds");

		startTime = System.currentTimeMillis();
		JavaRDD<IntArray> transactions = inputLines.map(transactionParser);
		do {
			JavaPairRDD<Integer, Integer> transactionsMapped;
			if (firstRound) {
				transactionsMapped = transactions.flatMapToPair(singleItemsExtractor);
			} else {
				CandidateMatcherTrie myCandidateMatcher = new CandidateMatcherTrie(trie);
				transactionsMapped = transactions.flatMapToPair(myCandidateMatcher);
			}
			JavaPairRDD<Integer, Integer> reducedTransactions = transactionsMapped.reduceByKey(reducer);
			List<Tuple2<Integer, Integer>> collectedItems = reducedTransactions.collect();
			
			//count confidence
			List<Tuple2<IntArray, Integer>> collectedItemSets = Main.spellOutLargeItems(collectedItems, firstRound);
			for (Tuple2<IntArray, Integer> tuple : collectedItemSets) {
				allSupport.put(tuple._1.valueSet(), tuple._2);
			}
			
			JavaPairRDD<Integer, Integer> filteredSupport = reducedTransactions.filter(minSupportFilter);
			List<Tuple2<Integer, Integer>> collected = filteredSupport.collect();
			System.out.println("the map-reduce-step took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");

			startTime = System.currentTimeMillis();
			List<Tuple2<IntArray, Integer>> largeItemSets= Main.spellOutLargeItems(collected, firstRound);
			
			Set<IntArray> largeItems = new HashSet<IntArray>(largeItemSets.size());
			for (Tuple2<IntArray, Integer> tuple : largeItemSets) {
				largeItems.add(tuple._1);
			}
			
			candidates = generateCandidatesInt(largeItems);
			if (!secondRound) {
				Set<IntArray> candidates1 = generateCandidatesInt(candidates);
				Set<IntArray> candidates2 = generateCandidatesInt(candidates1);
				candidates.addAll(candidates1);
				candidates.addAll(candidates2);
			}
			largeItemss.addAll(candidates);
			if (candidates.size() > 0) {
				trie = Main.candidatesToTrie(candidates);
			}
			secondRound = firstRound;
			firstRound = false;
			
			System.out.println("");
			System.out.println("the candidate generation took " + 
							  ((System.currentTimeMillis() - startTime) / 1000) + 
							  " seconds and generated " + candidates.size() + 
							  " candidates");
		} while (candidates.size() > 0);
	}
}
