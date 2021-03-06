package de.hpi.dbda;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFields;
import org.apache.flink.api.java.tuple.Tuple2;

@ReadFields("f1")
public class MinSupportFilter<T> implements FilterFunction<Tuple2<T, Integer>> {
	private static final long serialVersionUID = -4954745765332298766L;
	private int minSupport;

	public MinSupportFilter(int minSupport) {
		this.minSupport = minSupport;
	}

	public boolean filter(Tuple2<T, Integer> input){
		return input.f1 >= minSupport;
	}
};
