package de.hpi.dbda;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

public class Q3 {
	
	
	public static Map<Integer, String> compressionMapping = new HashMap<Integer, String>();
	public static Set largeItems = new HashSet();
	public static HashMap<Set,Integer> allSupport = new HashMap<Set, Integer>();
	public static double minConf;
	public static int minSupport;
	public static int minRating;
	
	public static boolean checkARConfidenceAndSupport(AssociationRule ar) {
		HashSet f = ar.first;
		HashSet l = ar.last;
		HashSet a = (HashSet) f.clone();
		a.addAll(l);
		double confidence = 0;
		if (!allSupport.containsKey(a)) {

		} else if (!allSupport.containsKey(f)) {
		} else {
			confidence = (double) allSupport.get(a) / (double) allSupport.get(f);
			if (allSupport.get(a) < minSupport) {
				confidence = 0;
			}
		}
		
		return confidence > minConf;
	}
	
	public static Set<List<String>> generateCandidates(
			List<Tuple2<List<String>, Integer>> largeItemTuples) {
		
		Set<List<String>> largeItems = new HashSet<List<String>>(largeItemTuples.size());
		for (Tuple2<List<String>, Integer> tuple : largeItemTuples) {
			largeItems.add(tuple.f0);
			System.out.print(tuple.f0);
			System.out.print(", ");
		}

		HashSet<List<String>> result = new HashSet<List<String>>();
		MultiValueMapWithArrayList map = new MultiValueMapWithArrayList();
		
		for (int i = 0; i < largeItemTuples.size(); i++) {
			ListPointer key = new ListPointer(
					largeItemTuples.get(i).f0, largeItemTuples.get(i).f0.size() - 1);
			
			map.put(key, largeItemTuples.get(i).f0);
		}

		for (Object key : map.keySet()) {
			ArrayList<List<String>> entrySet = (ArrayList<List<String>>) map.getCollection(key);
			for (int i = 0; i < entrySet.size() - 1; i++) {
				for (int j = i + 1; j < entrySet.size(); j++) {
					List<String> candidate;
					if (entrySet.get(i).get(entrySet.get(i).size() - 1)
							.compareTo(entrySet.get(j)
							.get(entrySet.get(j).size() - 1)) < 0) {
								// if the item set at position i is "smaller than" the item set at position j
						candidate = new ArrayList<String>(entrySet.get(i));
						candidate.add(entrySet.get(j).get(entrySet.get(j).size() - 1));
					} else {
						candidate = new ArrayList<String>(entrySet.get(j));
						candidate.add(entrySet.get(i).get(entrySet.get(i).size() - 1));
					}

					boolean keepCandidate = true;
					for (int ignoreIndex = 0; ignoreIndex < candidate.size() - 2; ignoreIndex++) {
						LinkedList<String> subset = new LinkedList<String>(candidate);
						subset.remove(ignoreIndex);
						if (!largeItems.contains(subset)) {
							keepCandidate = false;
							break;
						}
					}
					if (keepCandidate) {
						result.add(candidate);
					}
				}
			}
		}

		return result;
	}
	
	private static void aprioriOnStrings(String[] args, ExecutionEnvironment env) throws Exception {
		
		MapFunction<String, List<String>> transactionParser = new MapFunction<String, List<String>>() {
			private static final long serialVersionUID = -4625524329716723997L;

			public List<String> map(String line) {
				String[] items = line.split(" ");
				if (items.length > 0) {
					Arrays.sort(items);
					return Arrays.asList(items);
				} else {
					return new ArrayList<String>(1);
				}
			}

		};

		FlatMapFunction<List<String>, Tuple2<List<String>, Integer>> singleItemsExtractor = 
				new FlatMapFunction<List<String>, Tuple2<List<String>, Integer>>() {
			private static final long serialVersionUID = -6501622718277608505L;

			public void flatMap(List<String> transaction, Collector<Tuple2<List<String>,Integer>> out) {

				for (String item : transaction) {
					List<String> itemList = new ArrayList<String>();
					itemList.add(item);
					out.collect(new Tuple2<List<String>, Integer>(itemList, 1));
				}
			}

		};

		GroupReduceFunction<Tuple2<List<String>, Integer>, Tuple2<List<String>, Integer>> reducer = 
				new GroupReduceFunction<Tuple2<List<String>, Integer>, Tuple2<List<String>, Integer>>() {
			private static final long serialVersionUID = 9090470139360266644L;

			public void reduce(Iterable<Tuple2<List<String>, Integer>> v,
					Collector<Tuple2<List<String>, Integer>> out) {
				int sum = 0;
				List<String> list = null;
				for (Tuple2<List<String>,Integer> i : v) {
					sum += i.f1;
					list = i.f0;
					
				}
				out.collect(new Tuple2<List<String>, Integer>(list,sum));
			}
		};

		FilterFunction<Tuple2<List<String>, Integer>> minSupportFilter = 
				new MinSupportFilter<List<String>>(Q3.minSupport);

		boolean firstRound = true;
		Set<List<String>> candidates = null;
		long startTime = System.currentTimeMillis();
		DataSet<String> inputLines = env.readTextFile(args[0]);
		DataSet<List<String>> transactions = inputLines.map(transactionParser);

		do {
			DataSet<Tuple2<List<String>, Integer>> transactionsMapped;
			if (firstRound) {
				firstRound = false;
				transactionsMapped = transactions.flatMap(singleItemsExtractor);
			} else {
				CandidateMatcher myCandidateMatcher = new CandidateMatcher(candidates);
				transactionsMapped = transactions.flatMap(myCandidateMatcher);
			}
			DataSet<Tuple2<List<String>, Integer>> reducedTransactions = transactionsMapped.groupBy(
					new KeySelector<Tuple2<List<String>,Integer>,String>(){
						private static final long serialVersionUID = 127074927072828508L;

						public String getKey(Tuple2<List<String>,Integer> t) {
							String result = StringUtils.join(t.f0, " ");
							return result;
						}
					}).reduceGroup(reducer);
			DataSet<Tuple2<List<String>, Integer>> filteredSupport = 
					reducedTransactions.filter(minSupportFilter);
			List<Tuple2<List<String>, Integer>> collected = filteredSupport.collect();

			for (Tuple2<List<String>, Integer> tuple : collected) {
				allSupport.put(new HashSet<String>(tuple.f0), tuple.f1);
			}

			System.out.println("the map-reduce-step took " + 
					((System.currentTimeMillis() - startTime) / 1000) + " seconds");

			startTime = System.currentTimeMillis();
			candidates = generateCandidates(collected);
			largeItems.addAll(candidates);
			System.out.println("the candidate generation took " + 
					((System.currentTimeMillis() - startTime) / 1000) + 
					" seconds and generated " + candidates.size() + 
					" candidates");
		} while (candidates.size() > 0);
	}

	public static void main(String[] args) throws Exception {

        if (args.length < 6) {
            System.out.println("please provide the following parameters: input_path, " +
            		"result_file_path, modus, minSupport, minRating and minConfidence");
            System.out.println("for example:");
            System.out.println("./spark-submit --master spark://172.16.21.111:7077 " +
            		"--driver-memory 7g --conf spark.executor.memory=4g " +
            		"--class de.hpi.dbda.Main " +
            		"/home/martin.gebert/Apriori_Data_Mining-0.0.1-SNAPSHOT.jar " +
            		"hdfs://tenemhead2:8020/data/netflix.txt " +
            		"/home/martin.gebert/result.txt ints 8000 5 0.5");
            System.exit(0);
        }
        
		minSupport = Integer.parseInt(args[3]);
		minRating = Integer.parseInt(args[4]);
		minConf = Double.parseDouble(args[5]);

		final ExecutionEnvironment env = ExecutionEnvironment
		        .getExecutionEnvironment();

        switch(args[2]) {
            case "strings":
            	aprioriOnStrings(args, env);
            break;
            case "ints":
            //aprioriOnInts(args, context);
            break;
            case "trie":
            //aprioriOnIntsWithTrie(args, context);
            break;
            case "fpc":
            //FPC.fpcOnIntsWithTrie(args, context);
            //Main.allSupport = FPC.allSupport;
            //Main.largeItems = FPC.largeItemss;
            break;
            default:
            System.out.println("unknown mode");
            break;
        }
       
        ArrayList<AssociationRule> ar = new ArrayList<AssociationRule>();
        //CHANGE IF FPC - IntArray candidate : FPC.largeItemss
        //for (IntArray candidate : largeItems) {

		if (largeItems.size() > 0 && largeItems.iterator().next() instanceof IntArray) {
			for (IntArray candidate : (Set<IntArray>) largeItems) {
				HashSet<HashSet<Integer>> pow = Utilities.powerSet(candidate);
				pow.remove(new HashSet<Integer>());
				pow.remove(candidate.valueSet());

				for (HashSet<Integer> s : pow) {
					HashSet<Integer> o = candidate.valueSet();
					o.removeAll(s);
					AssociationRule<Integer> asr = new AssociationRule<Integer>(s, o);
					if (checkARConfidenceAndSupport(asr)) {
						System.out.println(Utilities.printAssociationRule(asr));
						ar.add(asr);
					}
				}
			}
		}
		else if (largeItems.size() > 0 && largeItems.iterator().next() instanceof List<?>) {
			for (List<String> candidate : (Set<List<String>>) largeItems) {
				HashSet<HashSet<String>> pow = Utilities.powerSet(candidate);
				pow.remove(new HashSet<String>());
				pow.remove(new HashSet<String>(candidate));

				for (HashSet<String> s : pow) {
					HashSet<String> o = new HashSet<String>(candidate);
					o.removeAll(s);
					AssociationRule<String> asr = new AssociationRule<String>(s, o);
					if (checkARConfidenceAndSupport(asr)) {
						System.out.println(Utilities.printAssociationRule(asr));
						ar.add(asr);
					}
				}
			}
		}
        
        File file = new File(args[1]);
         
        // if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        for (AssociationRule arule : ar) {
            bw.write(Utilities.printAssociationRule(arule));
        }
        bw.close();

    }
	
	

  public static void main2(String[] args) throws Exception {
    // get job parameters
    final String lineItemFile = args[0];
    final String ordersFile = args[1];
    final String resultFile = args[2];
    final String DATE = "1995-03-15";

    // set up the execution environment
    final ExecutionEnvironment env = ExecutionEnvironment
        .getExecutionEnvironment();

    // load lineitems
    DataSet<LineItem> lineItems = env.readTextFile(lineItemFile)
    // map to lineitem
        .map(new MapFunction<String, LineItem>() {
          @Override
          public LineItem map(String line) throws Exception {
            return new LineItem(line);
          }
        })
        // filter by SHIPDATE>DATE
        .filter(new FilterFunction<LineItem>() {
          @Override
          public boolean filter(LineItem li) throws Exception {
            return li.SHIPDATE().compareTo(DATE) > 0;
          }
        });

    // aggregate sum(PRICE *(1-DISCOUNT)) by ORDERKEY
    DataSet<Tuple2<Integer, Double>> lineItemRevenue = lineItems
        .map(new MapFunction<Q3.LineItem, Tuple2<Integer, Double>>() {
          @Override
          public Tuple2<Integer, Double> map(LineItem li) throws Exception {
            return new Tuple2<Integer, Double>(li.ORDERKEY(), li.PRICE() * (1 - li.DISCOUNT()));
          }
        }).groupBy(0).sum(1);

    // load orders
    DataSet<Order> orders = env.readTextFile(ordersFile)
    // map to order
        .map(new MapFunction<String, Order>() {
          @Override
          public Order map(String line) throws Exception {
            return new Order(line);
          }
          // filter by SHIPDATE>DATE
        }).filter(new FilterFunction<Order>() {
          @Override
          public boolean filter(Order order) throws Exception {
            return order.ORDERDATE().compareTo(DATE) < 0;
          }
        });

    // join by ORDERKEY
    DataSet<Tuple2<Integer, Double>> joined = lineItemRevenue
        .join(orders)
        .where(0)
        .equalTo(0)
        // modify / cleanup tuple format
        .map(new MapFunction<Tuple2<Tuple2<Integer, Double>, Order>, Tuple2<Integer, Double>>() {
          @Override
          public Tuple2<Integer, Double> map(Tuple2<Tuple2<Integer, Double>,Order> value) throws Exception {
            return value.f0;
          }
        })
        // sort by revenue (desc) <-- only partition-wise
        .sortPartition(1, org.apache.flink.api.common.operators.Order.DESCENDING)
        ;
    // save results
    joined.writeAsCsv(resultFile, "\n", "\t", WriteMode.OVERWRITE);

    //*
    // execute program
    env.execute("TPC-H Q3");
    /*/
    System.out.println(env.getExecutionPlan());
    //*/
  }
  
  public static class LineItem extends Tuple5<Integer, Integer, Double, Double, String> 
  	implements Serializable {
    Integer ORDERKEY() {return f0;};
    Integer ITEM() {return f1;};
    Double PRICE() {return f2;};
    Double DISCOUNT() {return f3;};
    String SHIPDATE() {return f4;};
    // define default constructor (used for deserialization)
    public LineItem() { }
    public LineItem(String line) {
      this(line.split("\\|"));
    }
    public LineItem(String[] values) {
      super(
        Integer.parseInt(values[0]),
        Integer.parseInt(values[3]),
        Double.parseDouble(values[5]),
        Double.parseDouble(values[6]),
        values[10]);
    }
  }
  
  public static class Order extends Tuple2<Integer, String> implements Serializable {
    Integer ORDERKEY() {return f0;};
    String ORDERDATE() {return f1;};
    // define default constructor (used for deserialization)
    public Order() { }
    public Order(String line) {
      this(line.split("\\|"));
    }
    public Order(String[] values) {
      super(Integer.parseInt(values[0]), values[4]);
    }
  }
}
