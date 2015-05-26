package de.hpi.dbda;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class IntArray implements Serializable, Comparable<IntArray> {
	private static final long serialVersionUID = 9178408728199032638L;

	public int[] value;

	public IntArray(int[] value) {
		this.value = value;
	}

	public IntArray(IntArray original) {
		this.value = Arrays.copyOf(original.value, original.value.length);
	}

	public IntArray(IntArray original, int pushElement) {
		this.value = Arrays.copyOf(original.value, original.value.length + 1);
		this.value[this.value.length - 1] = pushElement;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof IntArray == false) {
			return false;
		}
		return Arrays.equals(((IntArray) obj).value, this.value);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(this.value);
	}

	// this leads to a lexicographic ordering:
	public int compareTo(IntArray other) {
		if (other == null) {
			throw new NullPointerException();
		}

		int difference;
		for (int i = 0; i < this.value.length && i < other.value.length; i++) {
			difference = this.value[i] - other.value[i];
			if (difference != 0) {
				return difference;
			}
		}
		return this.value.length - other.value.length;
	}
	
	public void print() {
		for (int i : value) {
			System.out.print(i);
			System.out.print(" ");
		}
	}
	
	public int length() {
		return value.length;
	}
	
	public Integer head() {
		return value[0];
	}
	
	public IntArray rest() {
		return new IntArray(Arrays.copyOfRange(value, 1, value.length));
	}
	
	public IntArray clone() {
		return new IntArray(value);
	}
	
	public HashSet<Integer> valueSet() {
		HashSet<Integer> n = new HashSet<Integer>();
		for (int i : value) {
			n.add(i);
		}
		return n;
	}

}
