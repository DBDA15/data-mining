package DBDA_Gebert_Perchyk.Data_Mining_Simple;

import java.util.Set;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class CovarianceCalculator implements Function<Tuple2<Set<String>, Tuple2<Integer, Integer>>, Tuple2<Set<String>, Float>> {
	private static final long serialVersionUID = 7453514131197731990L;

	private int lineCount;

	public CovarianceCalculator(int lineCount) {
		this.lineCount = lineCount;
	}

	public Tuple2<Set<String>, Float> call(Tuple2<Set<String>, Tuple2<Integer, Integer>> entry) throws Exception {
		float observedCount = entry._2._1;
		float expectedCount = entry._2._2;
		return new Tuple2<Set<String>, Float>(entry._1, observedCount / lineCount - expectedCount / lineCount / lineCount);
	}
}
