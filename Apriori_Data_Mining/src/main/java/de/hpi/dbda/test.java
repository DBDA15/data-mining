package de.hpi.dbda;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


public class test {

	public static<T> HashSet<HashSet<T>> powerSet(List<T> originalSet) {
		HashSet<HashSet<T>> sets = new HashSet<HashSet<T>>();
        if (originalSet.size() == 0) {
            sets.add(new HashSet<T>());
            return sets;
        }
        T head = originalSet.get(0);
		List<T> rest = originalSet.size() > 1 ? originalSet.subList(1, originalSet.size()) : new ArrayList<T>(0);
        for (HashSet<T> set : powerSet(rest)) {
        	HashSet<T> newSet = new HashSet<T>();
            newSet.add(head);
            newSet.addAll(set);
            sets.add(newSet);
            sets.add(set);
        }
        return sets;
    }

}
