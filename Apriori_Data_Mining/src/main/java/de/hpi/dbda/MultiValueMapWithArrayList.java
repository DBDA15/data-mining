package de.hpi.dbda;

import org.apache.commons.collections.FactoryUtils;
import org.apache.commons.collections.map.MultiValueMap;

import java.util.ArrayList;
import java.util.HashMap;


public class MultiValueMapWithArrayList extends MultiValueMap {
	public MultiValueMapWithArrayList () {
		super(new HashMap(), FactoryUtils.instantiateFactory(new ArrayList().getClass()));
	}
}