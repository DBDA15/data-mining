package de.hpi.dbda;

import java.io.ByteArrayInputStream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import de.hpi.dbda.trie.InnerTrieNode;

public class CandidateMatcherTrie extends RichFlatMapFunction<IntArray, Tuple2<Integer, Integer>> {
	private static final long serialVersionUID = -5812427046006186493L;
	InnerTrieNode trie;

	@Override
	public void open(org.apache.flink.configuration.Configuration parameters)
			throws Exception {
		byte[] bytes = (byte[]) getRuntimeContext().getBroadcastVariable(
				TrieBuilder.TRIE_NAME).get(0);

		System.out.println(bytes.length);
		
		Kryo kryo = new Kryo();
    	//kryo.register(InnerTrieNode.class);
    	//kryo.setRegistrationRequired(false);

		//kryo.readClassAndObject(new Input(new ByteArrayInputStream(bytes)));
		trie = kryo.readObject(new Input(new ByteArrayInputStream(bytes)), InnerTrieNode.class);
	}

	public void flatMap(IntArray transactionWrapper, Collector<Tuple2<Integer, Integer>> out) {
		int[] transaction = transactionWrapper.value;
		traverseTrie(transaction, 0, trie, out);
	}

	private void traverseTrie(int[] transaction, int processedElements, InnerTrieNode trieNode, Collector<Tuple2<Integer, Integer>> out) {
		for (int labelIndex = 0; labelIndex < trieNode.edgeLabels.length; labelIndex++) {
			while (transaction[processedElements] < trieNode.edgeLabels[labelIndex]) {
				processedElements++;
				if (processedElements == transaction.length) {
					return;
				}
			}

			if (transaction[processedElements] == trieNode.edgeLabels[labelIndex]) {
				InnerTrieNode childNode = trieNode.children[labelIndex];
				if (childNode.candidateID != -1) {
					out.collect(new Tuple2<Integer, Integer>(childNode.candidateID, 1));
				}
				if (childNode.children != null) {
					// At least one more transaction element must be matched against the trie. 
					// If no transaction element is left, this is impossible.
					if (processedElements + 1 < transaction.length) { 
						traverseTrie(transaction, processedElements + 1,
									 (InnerTrieNode) childNode, out);
					}
				}
			}
		}
	}

}
