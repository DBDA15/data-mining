package de.hpi.dbda;

import org.apache.commons.collections.FactoryUtils;
import org.apache.commons.collections.map.MultiValueMap;

import java.util.ArrayList;
import java.util.HashMap;


public class MVMap extends MultiValueMap {
	public MVMap () {
		super(new HashMap(), FactoryUtils.instantiateFactory(new ArrayList().getClass()));
	}
}