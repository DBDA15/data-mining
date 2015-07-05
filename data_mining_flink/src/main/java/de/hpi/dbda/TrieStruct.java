package de.hpi.dbda;

import java.io.Serializable;
import java.util.List;

public class TrieStruct implements Serializable {
	private static final long serialVersionUID = -4117847696175879528L;
	public byte[] trie;
	public List<IntArray> candidateLookup;

	TrieStruct(byte[] trie, List<IntArray> candidateLookup) {
		this.trie = trie;
		this.candidateLookup = candidateLookup;
	}

}
