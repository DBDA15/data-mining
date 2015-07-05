package de.hpi.dbda.triejobs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import de.hpi.dbda.IntArray;

public class SingleItemsExtractor implements FlatMapFunction<IntArray, Tuple2<Integer, Integer>> {
	private static final long serialVersionUID = 4206575656443369070L;

	public void flatMap(IntArray transaction, Collector<Tuple2<Integer, Integer>> out) throws Exception {
		for (int item : transaction.value) {
			out.collect(new Tuple2<Integer, Integer>(item, 1));
		}
	}

}
