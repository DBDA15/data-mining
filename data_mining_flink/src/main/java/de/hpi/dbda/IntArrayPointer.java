package de.hpi.dbda;

import java.util.Objects;

public class IntArrayPointer {
	private IntArray listElements;
	private static final int hashcode = 1938638499;
	private int ignoreIndex;

	public IntArrayPointer(IntArray el, int ignoreIndex) {
		listElements = el;
		this.ignoreIndex = ignoreIndex;
	}

	@Override
	public int hashCode() {
		int hash = hashcode;
		for (int i = 0; i < listElements.value.length; i++) {
			if (i == ignoreIndex) {
				continue;
			}
			hash = Objects.hash(hash, listElements.value[i]);
		}
		return hash;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (o instanceof IntArrayPointer == false)
			return false;
		IntArrayPointer otherList = (IntArrayPointer) o;
		// if(this.listElements.size())
		int myIndex = 0;
		int otherIndex = 0;
		for (int i = 0; i < this.listElements.value.length - 1; i++) {
			if (myIndex == this.ignoreIndex) {
				myIndex++;
			}
			if (otherIndex == otherList.ignoreIndex) {
				otherIndex++;
			}
			if (this.listElements.value[myIndex] != otherList.listElements.value[otherIndex]) {
				return false;
			}
			myIndex++;
			otherIndex++;
		}
		return true;
	}

}
