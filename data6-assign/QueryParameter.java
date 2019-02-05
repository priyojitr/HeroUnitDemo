package com.stackroute.datamunger.query.parser;

import java.util.List;

/* 
 * This class will contain the elements of the parsed Query String such as conditions,
 * logical operators,aggregate functions, file name, fields group by fields, order by
 * fields, Query Type
 * */
public class QueryParameter {


	private String queryString;
	private String fileName;
	private String baseQuery;
	private List<String> fields;
	private String QUERY_TYPE;
	private List<Restriction> restrictions;
	private List<String> logicalOperators;
	private List<AggregateFunction> aggregateFunctions;
	private List<String> orderByFields;
	private List<String> groupByFields;

	public String getQueryString() {
		return queryString;
	}

	public String getFileName() {
		return fileName;
	}

	public void setQueryString(final String queryString) {
		this.queryString = queryString;
	}

	public void setFileName(final String fileName) {
		this.fileName = fileName;
	}

	public void setBaseQuery(final String baseQuery) {
		this.baseQuery = baseQuery;
	}

	public void setFields(final List<String> fields) {
		this.fields = fields;
	}

	public void setQUERY_TYPE(final String QUERY_TYPE) {
		this.QUERY_TYPE = QUERY_TYPE;
	}

	public void setRestrictions(final List<Restriction> restrictions) {
		this.restrictions = restrictions;
	}

	public void setLogicalOperators(final List<String> logicalOperators) {
		this.logicalOperators = logicalOperators;
	}

	public void setAggregateFunctions(final List<AggregateFunction> aggregateFunctions) {
		this.aggregateFunctions = aggregateFunctions;
	}

	public void setOrderByFields(final List<String> orderByFields) {
		this.orderByFields = orderByFields;
	}

	public void setGroupByFields(final List<String> groupByFields) {
		this.groupByFields = groupByFields;
	}

	public String getBaseQuery() {
		return baseQuery;
	}

	public List<Restriction> getRestrictions() {
		return restrictions;
	}

	public List<String> getLogicalOperators() {
		return logicalOperators;
	}

	public List<String> getFields() {
		return fields;
	}

	public List<AggregateFunction> getAggregateFunctions() {
		return aggregateFunctions;
	}

	public List<String> getGroupByFields() {
		return groupByFields;
	}

	public List<String> getOrderByFields() {
		return orderByFields;
	}

	public String getQUERY_TYPE() {
		return QUERY_TYPE;
	}

	public QueryParameter() {
		// TODO Auto-generated constructor stub
	}
	

}
