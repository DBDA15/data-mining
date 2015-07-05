package de.hpi.dbda.triejobs;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFields;

import de.hpi.dbda.IntArray;
import de.hpi.dbda.TrieStruct;

public class LookupExtractor implements MapFunction<TrieStruct, List<IntArray>> {
	private static final long serialVersionUID = -2145546767567472047L;
	
	@Override
	public List<IntArray> map(TrieStruct value) throws Exception {
		return value.candidateLookup;
	}

}
