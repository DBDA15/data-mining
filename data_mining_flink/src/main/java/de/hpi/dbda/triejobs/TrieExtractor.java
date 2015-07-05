package de.hpi.dbda.triejobs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFields;

import de.hpi.dbda.TrieStruct;

public class TrieExtractor implements MapFunction<TrieStruct, byte[]> {
	private static final long serialVersionUID = -2145638643793472047L;

	@Override
	public byte[] map(TrieStruct value) throws Exception {
		return value.trie;
	}

}
