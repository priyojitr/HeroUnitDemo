package com.stackroute.datamunger.query;

import java.util.Comparator;

/*
 * The GenericComparator class implements Comparator Interface. This class is used to 
 * compare row objects which will be used for sorting the dataSet
 */
public class GenericComparator implements Comparator<Row> {
	public String key;

    public GenericComparator(String key) {
        this.key = key;
    }
@Override
    public int compare(Row o1, Row o2) {
    		return new Integer(o1.get(key).compareTo(o2.get(key)));
	}

}
