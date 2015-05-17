package de.hpi.dbda.trie;

public class TrieLeaf implements TrieNode {
	private static final long serialVersionUID = -6288974034357570841L;
	public int value;

	public TrieLeaf(int value) {
		this.value = value;
	}
}
