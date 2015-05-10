package de.hpi.dbda;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class CandidateMatcher implements PairFlatMapFunction<List<String>, List<String>, Integer> {
	private static final long serialVersionUID = -8691451199544626471L;
	Set<List<String>> candidates;

	public CandidateMatcher(Set<List<String>> candidates) {
		this.candidates = candidates;
	}

	public Iterable<Tuple2<List<String>, Integer>> call(List<String> transaction) throws Exception {
		HashSet<Tuple2<List<String>, Integer>> result = new HashSet<Tuple2<List<String>, Integer>>();

		for (List<String> myCandidate : candidates) {
			int j = 0;
			for (int i = 0; i < myCandidate.size(); i++) {
				int comparisonResult = -1; // initial value is important in case the following for loop isn't executed because j is too great
				for (; j < transaction.size(); j++) {
					comparisonResult = transaction.get(j).compareTo(myCandidate.get(i));
					if (comparisonResult >= 0) {
						break;
					}
				}
				if (comparisonResult != 0) { // comparisonResult < 0 means the end of the transaction was reached but candidate.get(i) wasn't found
												// comparisonResult > 0 means the transaction doesn't contain candidate.get(i) (, but an element greater than candidate.get(i) )
					break;
				}

				if (i == myCandidate.size() - 1) { // if all elements of candidate were found in transaction
					result.add(new Tuple2<List<String>, Integer>(myCandidate, 1));
				}
			}
		}

		return result;
	}

}