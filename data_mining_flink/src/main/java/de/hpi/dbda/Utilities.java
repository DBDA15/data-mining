package de.hpi.dbda;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class Utilities {

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
	
	public static String printAssociationRule(AssociationRule ar){
		String al = "";
		Set f = ar.first;
		Set l = ar.last;
		String itemString;
		for (Object i : f) {
			itemString = (String) (i instanceof String ? i : i.toString());
			al += itemString + ", ";
		}
		al += " => ";
		for (Object i : l) {
			itemString = (String) (i instanceof String ? i : i.toString());
			al += itemString + ", ";
		}
		al += "\n";
		return al;
	}

}
