package de.hpi.dbda;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class CandidatesListMatcher implements PairFlatMapFunction<IntArray, IntArray, Integer> {
	private static final long serialVersionUID = -7181647483073087875L;
	Set<IntArray> candidates;

	public CandidatesListMatcher(Set<IntArray> candidates) {
		this.candidates = candidates;
	}

	public Iterable<Tuple2<IntArray, Integer>> call(IntArray transactionWrapper) throws Exception {
		HashSet<Tuple2<IntArray, Integer>> result = new HashSet<Tuple2<IntArray, Integer>>();

		int[] myCandidate;
		int[] transaction = transactionWrapper.value;
		for (IntArray myCandidateWrapper : candidates) {
			myCandidate = myCandidateWrapper.value;
			int j = 0;
			for (int i = 0; i < myCandidate.length; i++) {
				int comparisonResult = -1; // initial value is important in case the following for loop isn't executed because j is too great
				for (; j < transaction.length; j++) {
					comparisonResult = transaction[j] - myCandidate[i];
					if (comparisonResult >= 0) {
						break;
					}
				}
				if (comparisonResult != 0) { // comparisonResult < 0 means the end of the transaction was reached but candidate.get(i) wasn't found
												// comparisonResult > 0 means the transaction doesn't contain candidate.get(i) (, but an element greater than candidate.get(i) )
					break;
				}

				if (i == myCandidate.length - 1) { // if all elements of candidate were found in transaction
					result.add(new Tuple2<IntArray, Integer>(myCandidateWrapper, 1));
				}
			}
		}

		return result;
	}

}