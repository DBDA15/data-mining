package de.hpi.dbda;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class MinSupportFilter<T> implements Function<Tuple2<T, Integer>, Boolean> {
	private static final long serialVersionUID = 2699661581091927958L;
	private int minSupport = Main.minSupport;

	public MinSupportFilter(int minSupport) {
		this.minSupport = minSupport;
	}

	public Boolean call(Tuple2<T, Integer> input) throws Exception {
		return input._2 >= minSupport;
	}
};
