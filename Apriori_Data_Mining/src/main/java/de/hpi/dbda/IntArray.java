package de.hpi.dbda;

import java.io.Serializable;
import java.util.Arrays;

public class IntArray implements Serializable {
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
}
