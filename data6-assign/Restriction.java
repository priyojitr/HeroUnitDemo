package com.stackroute.datamunger.query.parser;

/*
 * This class is used for storing name of field, condition and value for 
 * each conditions  and mention parameterized constructor
 * */
public class Restriction {
	private String propertyName;
	private String propertyValue;
	private String condition;
	public String getPropertyName() {
		// TODO Auto-generated method stub
		return propertyName;
	}

	public String getPropertyValue() {
		// TODO Auto-generated method stub
		return propertyValue;
	}

	public String getCondition() {
		// TODO Auto-generated method stub
		return condition;
	}
	public void setPropertyName(final String propertyName) {
		this.propertyName = propertyName;
	}


	public void setPropertyValue(final String propertyValue) {
		this.propertyValue = propertyValue;
	}


	public void setCondition(final String condition) {
		this.condition = condition;
	}
	
	// Write logic for constructor
	public Restriction(final String name, final String value, final String condition) {
		propertyName = name;
		propertyValue = value;
		this.condition = condition;
	}


	@Override
	public String toString() {
		return "Restriction [propertyName=" + propertyName + ", propertyValue="
				+ propertyValue + ", condition=" + condition + "]";
	}
	

}
