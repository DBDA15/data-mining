package de.hpi.dbda;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Reducer<T> implements GroupReduceFunction<Tuple2<T, Integer>, Tuple2<T, Integer>> {
	private static final long serialVersionUID = 7041924124453286838L;

	public void reduce(Iterable<Tuple2<T, Integer>> v, Collector<Tuple2<T, Integer>> out) {
		int sum = 0;
		T list = null;
		for (Tuple2<T, Integer> i : v) {
			sum += i.f1;
			list = i.f0;

		}
		out.collect(new Tuple2<T, Integer>(list, sum));
	}
}