package de.hpi.dbda.triejobs;

import java.util.Arrays;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFields;

import de.hpi.dbda.IntArray;

public class TransactionParser implements MapFunction<String, IntArray> {
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

}