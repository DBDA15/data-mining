package DBDA_Gebert_Perchyk.Data_Mining_Simple;

import java.util.Set;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class WordSetCountFilterer implements Function<Tuple2<Set<String>, Integer>, Boolean> {
	private static final long serialVersionUID = 1320856446519692500L;
	private int targetWordCount;

	public WordSetCountFilterer(int targetWordCount) {
		this.targetWordCount = targetWordCount;
	}

	public Boolean call(Tuple2<Set<String>, Integer> wordSetCount) throws Exception {
		return wordSetCount._1.size() == targetWordCount;
	}

}
