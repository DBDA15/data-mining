package de.hpi.dbda;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import de.hpi.dbda.trie.InnerTrieNode;

public class TrieBuilder extends RichGroupReduceFunction<Tuple2<IntArray, Integer>, TrieStruct> {
	private static final long serialVersionUID = -8917906510553699266L;
	public static final String CANDIDATE_LOOKUP_NAME = "candidateLookup";
	public static final String TRIE_NAME = "trie";
	
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
        InnerTrieNode[] currentTriePath = new InnerTrieNode[sortedCandidates.iterator().next().value.length+1];
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
                        childCount++;
                } else if (firstDifferentElementIndices[i] < level) {
                        break;
                }
        }
        return childCount;
	}

	public static void printTrie(InnerTrieNode node) {
		if (node == null) {
			System.out.println("null");
			return;
		}
		if (node.candidateID != -1) {
			System.out.println("<" + candidateLookup.get((node).candidateID).printDecoded() + ">");
		} else {
			InnerTrieNode innerNode = (InnerTrieNode) node;
			System.out.println();
			for (int i = 0; i < innerNode.edgeLabels.length; i++) {
				System.out.print(innerNode.edgeLabels[i] + ", ");
			}
			for (int i = 0; i < innerNode.edgeLabels.length; i++) {
				printTrie(innerNode.children[i]);
			}
			System.out.println();
		}
	}

    @Override
	public void reduce(Iterable<Tuple2<IntArray, Integer>> largeItemsIterator,
			Collector<TrieStruct> out) throws Exception {
		
		List<Tuple2<IntArray,Integer>> largeItems = new ArrayList<Tuple2<IntArray, Integer>>();
		for(Tuple2<IntArray, Integer> largeItem:largeItemsIterator){
			largeItems.add(largeItem);
		}
    	
    	Set<IntArray> candidates = generateCandidatesInt(largeItems);
    	
    	System.out.println("candidates: "+candidates.size());
    	
    	if(candidates.size() == 0){
    		return;
    	}
    	
    	InnerTrieNode trie = candidatesToTrie(candidates);

    	// serialize trie
    	Kryo kryo = new Kryo();
		Output myOutput = new Output(new ByteArrayOutputStream());
		kryo.writeObject(myOutput, trie);
		byte[] buffer = myOutput.getBuffer();

		out.collect(new TrieStruct(buffer, candidateLookup));
	}

}
