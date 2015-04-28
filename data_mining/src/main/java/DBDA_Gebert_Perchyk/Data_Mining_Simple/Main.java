package DBDA_Gebert_Perchyk.Data_Mining_Simple;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Main {
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("please provide the following parameters: input_path and result_file_path");
			System.exit(0);
		}

		SparkConf sparkConf = new SparkConf().setAppName(Main.class.getName())/*.setMaster("local")*/;
		JavaSparkContext context = new JavaSparkContext(sparkConf);

		PairFlatMapFunction<String, Set<String>, Integer> mapper = new PairFlatMapFunction<String, Set<String>, Integer>() {
			private static final long serialVersionUID = 816387903261441637L;

			public Iterable<Tuple2<Set<String>, Integer>> call(String line) throws Exception {
				HashSet<Tuple2<Set<String>, Integer>> result = new HashSet<Tuple2<Set<String>, Integer>>();
				result.add(new Tuple2<Set<String>, Integer>(new HashSet<String>(), 1)); // add entry with (null, null) key for the line
				line = line.substring(line.indexOf('"') + 1, line.lastIndexOf('"')); // use only the abstract text, not the additional info
				String[] words = line.split("[^a-zA-Z0-9]+");
				Set<String> wordSet;
				for (int i = 0; i < words.length; i++) {
					wordSet = new HashSet<String>();
					wordSet.add(words[i]);
					result.add(new Tuple2<Set<String>, Integer>(wordSet, 1)); // add entry with (word1, null) key for the 1st word alone
					for (int k = i + 1; k < words.length; k++) {
						wordSet = new HashSet<String>();
						wordSet.add(words[i]);
						wordSet.add(words[k]);
						if (wordSet.size() != 1) { // if words[i] equals words[k], don't add a tuple for this "pair"
							result.add(new Tuple2<Set<String>, Integer>(wordSet, 1)); // add entry with (word1, word2) key for the word pair
						}
					}
				}

				return result;
			}
		};

		Function<String, Boolean> lineFilterer = new Function<String, Boolean>() {
			private static final long serialVersionUID = 4372559437013933640L;

			public Boolean call(String line) throws Exception {
				return line.length() > 0 && line.charAt(0) != '#';
			}
		};

		Function2<Integer, Integer, Integer> reducer = new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = -4611423471688162312L;

			public Integer call(Integer count1, Integer count2) throws Exception {
				return count1 + count2;
			}
		};

		PairFunction<Tuple2<Tuple2<Set<String>, Integer>, Tuple2<Set<String>, Integer>>, Set<String>, Integer> cartesianMapper = new PairFunction<Tuple2<Tuple2<Set<String>, Integer>, Tuple2<Set<String>, Integer>>, Set<String>, Integer>() {
			private static final long serialVersionUID = -6743791534949093821L;

			public Tuple2<Set<String>, Integer> call(Tuple2<Tuple2<Set<String>, Integer>, Tuple2<Set<String>, Integer>> wordCountsPair) throws Exception {
				Set<String> bothWords = new HashSet<String>();
				bothWords.addAll(wordCountsPair._1._1);
				bothWords.addAll(wordCountsPair._2._1);
				Tuple2<Set<String>, Integer> result = new Tuple2<Set<String>, Integer>(bothWords, wordCountsPair._1._2 * wordCountsPair._2._2);
				return result;
			}
		};

		Function<Tuple2<Set<String>, Integer>, Boolean> sameWordFilter = new Function<Tuple2<Set<String>, Integer>, Boolean>() {
			private static final long serialVersionUID = -2866473430516982667L;

			public Boolean call(Tuple2<Set<String>, Integer> entry) throws Exception {
				return entry._1.size() != 1;
			}
		};

		Function2<Integer, Integer, Integer> expectedCountsReducer = new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 7850654383077126202L;

			public Integer call(Integer count1, Integer count2) throws Exception {
				return count1;
			}
		};
		String resultFile = args[1];
		JavaRDD<String> lines = context.textFile(args[0]); // split input file into lines
		lines = lines.filter(lineFilterer); // filter out comment lines from the wikipedia file (this doesn't affect the other files)
		JavaPairRDD<Set<String>, Integer> allEntries = lines.flatMapToPair(mapper); // create 1 entry for every single word, every pair of co-occuring words and every line. The Integer value is always 1.
		JavaPairRDD<Set<String>, Integer> allCounts = allEntries.reduceByKey(reducer); // add up the counts for each element.
		int lineCount = allCounts.filter(new WordSetCountFilterer(0)).first()._2; // get the number of lines

		JavaPairRDD<Set<String>, Integer> singleWordCounts = allCounts.filter(new WordSetCountFilterer(1)); // get the counts for all single words
		JavaPairRDD<Tuple2<Set<String>, Integer>, Tuple2<Set<String>, Integer>> singleWordCartesian = singleWordCounts.cartesian(singleWordCounts); // build cartesian of all single words' counts
		JavaPairRDD<Set<String>, Integer> expectedCounts = singleWordCartesian.mapToPair(cartesianMapper); // for every pair of single words, calculate the product of their respective counts
		expectedCounts = expectedCounts.filter(sameWordFilter); // remove pairs where both single words are the same
		expectedCounts = expectedCounts.reduceByKey(expectedCountsReducer); // there are 2 equal entries for every pair of single words. Remove one of them for each pair.

		JavaPairRDD<Set<String>, Integer> observedCounts = allCounts.filter(new WordSetCountFilterer(2)); // get the counts for all co-occuring word pairs
		JavaPairRDD<Set<String>, Tuple2<Integer, Integer>> observedAndExpectedCounts = observedCounts.join(expectedCounts); // complement every count of two co-occuring words with the previously calculated product of their respective counts
		JavaRDD<Tuple2<Set<String>, Float>> covariances = observedAndExpectedCounts.map(new CovarianceCalculator(lineCount)); // calculate the covariances for every pair of co-occuring words

		covariances.saveAsTextFile(resultFile);
		context.close();
	}
}
