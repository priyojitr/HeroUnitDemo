package com.stackroute.datamunger.query.parser;

import java.util.ArrayList;
import java.util.List;

public class QueryParser {

	private final QueryParameter queryParameter = new QueryParameter();
	/*
	 * This method will parse the queryString and will return the object of
	 * QueryParameter class
	 */
	public QueryParameter parseQuery(String queryString) {
		
		queryParameter.setQueryString(queryString);
		
		String fileName = getFileName(queryString);
		queryParameter.setFileName(fileName);
		
		String baseQueryStr = getBaseQuery(queryString);
		queryParameter.setBaseQuery(baseQueryStr);
		
		List<String> orderByFields = getOrderByFields(queryString);
		queryParameter.setOrderByFields(orderByFields);
		
		List<String> groupByFields = getGroupByFields(queryString);
		queryParameter.setGroupByFields(groupByFields);
		
		List<String> selectedFieldsList = getFields(queryString);
		queryParameter.setFields(selectedFieldsList);
		
		List<Restriction> restrictionList = getConditionsPartQuery(queryString);
		queryParameter.setRestrictions(restrictionList);
		
		List<String> logicalOperatorList = getLogicalOperators(queryString);
		queryParameter.setLogicalOperators(logicalOperatorList);
		
		List<AggregateFunction> aggregateFunctionList = getAggregateFunctions(queryString);
		queryParameter.setAggregateFunctions(aggregateFunctionList);
		
		return queryParameter;
	
		
		
	}
	/*
	 * extract the name of the file from the query. File name can be found after the
	 * "from" clause.
	 */
		private String getFileName(final String queryString) {
		String fileName = null;
		final String searchStr = "from ";
		if(queryString != null && !queryString.isEmpty()) {
			final String fileNameStr = queryString.substring(queryString.indexOf(searchStr) + searchStr.length()).trim();
			if(fileNameStr != null && !fileNameStr.isEmpty()) {
				final String[] fileNameStrArr = fileNameStr.split(" ");
					fileName = fileNameStrArr[0];
			}
		}
		return fileName;
		}
	
		private String getBaseQuery(final String queryString) {
			String baseQueryStr = null;
			if(queryString != null && !queryString.isEmpty()) {
				if(queryString.contains("where")) {
					baseQueryStr = queryString.substring(0, queryString.indexOf("where") - 1).trim();
				} else if(queryString.contains("group by")) {
					baseQueryStr = queryString.substring(0, queryString.indexOf("group by") - 1).trim();
				} else if(queryString.contains("order by")) {
					baseQueryStr = queryString.substring(0, queryString.indexOf("order by") - 1).trim();
				} else {
					baseQueryStr = queryString;
				}
			}
			return baseQueryStr;
			}
		
	
	/*
	 * extract the order by fields from the query string. Please note that we will
	 * need to extract the field(s) after "order by" clause in the query, if at all
	 * the order by clause exists. For eg: select city,winner,team1,team2 from
	 * data/ipl.csv order by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one order by fields.
	 */
		private List<String> getOrderByFields(final String queryString) {
			String[] outputStrArr = null;
			final String searchOrderByStr = " order by ";
			if(queryString != null && !queryString.isEmpty()) {
				if(queryString.contains(searchOrderByStr)) {
					final String orderByStr = queryString.substring(queryString.indexOf(searchOrderByStr) + searchOrderByStr.length()).trim();
					if(orderByStr.contains(",")) {
						outputStrArr = orderByStr.split(",");
					} else {
						outputStrArr = new String[1];
						outputStrArr[0] = orderByStr.trim();
					}
				}
			}
			List<String> orderByFields = null;
			if(outputStrArr != null && outputStrArr.length > 0) {
				orderByFields = new ArrayList<String>();
				for(final String orderByField : outputStrArr) {
					orderByFields.add(orderByField);
				}
			}
			return orderByFields;
			}
	
	/*
	 * extract the group by fields from the query string. Please note that we will
	 * need to extract the field(s) after "group by" clause in the query, if at all
	 * the group by clause exists. For eg: select city,max(win_by_runs) from
	 * data/ipl.csv group by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one group by fields.
	 */
		private List<String> getGroupByFields(final String queryString) {
			String[] groupByFieldsArr = null;
			String groupByStr = null;
			final String searchGroupByStr = "group by";
			if(queryString != null && !queryString.isEmpty()) {
				if(queryString.contains(searchGroupByStr)) {
					if(queryString.contains("order by")) {
						groupByStr = queryString.substring(queryString.indexOf(searchGroupByStr) + searchGroupByStr.length(), queryString.indexOf("order by")).trim();
					} else {
						groupByStr = queryString.substring(queryString.indexOf(searchGroupByStr) + searchGroupByStr.length()).trim();
					}
				}
				if(groupByStr != null && !groupByStr.isEmpty()) {
					if(groupByStr.contains(",")) {
						groupByFieldsArr = groupByStr.split(",");
					} else {
						groupByFieldsArr = new String[1];
						groupByFieldsArr[0] = groupByStr.trim();
					}
				}
			}
			List<String> groupByFields = null;
			if(groupByFieldsArr != null && groupByFieldsArr.length > 0) {
				groupByFields = new ArrayList<String>();
				for(final String groupByField : groupByFieldsArr) {
					groupByFields.add(groupByField);
				}
			}
			return groupByFields;
			}
	
	/*
	 * extract the selected fields from the query string. Please note that we will
	 * need to extract the field(s) after "select" clause followed by a space from
	 * the query string. For eg: select city,win_by_runs from data/ipl.csv from the
	 * query mentioned above, we need to extract "city" and "win_by_runs". Please
	 * note that we might have a field containing name "from_date" or "from_hrs".
	 * Hence, consider this while parsing.
	 */
	
		private List<String> getFields(final String queryString) {
			String[] selectedFieldsArr = null;
			final String startSearchStr = "select ";
			final String endSerachStr = "from ";
			if(queryString != null && !queryString.isEmpty()) {
				if(queryString.contains(startSearchStr) && queryString.contains(endSerachStr)) {
					final String fieldNames = queryString.substring((queryString.indexOf(startSearchStr) + startSearchStr.length()),
							queryString.indexOf(endSerachStr)-1).trim();
					if(fieldNames.contains(",")) {
						selectedFieldsArr = fieldNames.split(",");
					} else {
						selectedFieldsArr = fieldNames.split(" ");
					}
				}
			}
			List<String> selectedFieldsList = null;
			
			if(selectedFieldsArr != null && selectedFieldsArr.length > 0) {
				selectedFieldsList = new ArrayList<String>();
				for(final String selectedField : selectedFieldsArr) {
					selectedFieldsList.add(selectedField.trim());
				}
			} else {
				selectedFieldsList = new ArrayList<String>();
				selectedFieldsList.add("*");
			}
			return selectedFieldsList;
			}
	
	
	/*
	 * extract the conditions from the query string(if exists). for each condition,
	 * we need to capture the following: 
	 * 1. Name of field 
	 * 2. condition 
	 * 3. value
	 * 
	 * For eg: select city,winner,team1,team2,player_of_match from data/ipl.csv
	 * where season >= 2008 or toss_decision != bat
	 * 
	 * here, for the first condition, "season>=2008" we need to capture: 
	 * 1. Name of field: season 
	 * 2. condition: >= 
	 * 3. value: 2008
	 * 
	 * the query might contain multiple conditions separated by OR/AND operators.
	 * Please consider this while parsing the conditions.
	 * 
	 */
	
		private List<Restriction> getConditionsPartQuery(final String queryString) {
			String[] conditionStrArr = null;

			String conditionStr = null;
			final String searchConditionStr = "where ";
			List<Restriction> restrictionList = null;
			if(queryString != null && !queryString.isEmpty()) {
				if(queryString.contains(searchConditionStr)) {
					if(queryString.contains("group by")) {
						conditionStr = queryString.substring((queryString.indexOf(searchConditionStr) + searchConditionStr.length()), queryString.indexOf("group by")).trim();
					} else if(queryString.contains("order by")) {
						conditionStr = queryString.substring((queryString.indexOf(searchConditionStr) + searchConditionStr.length()), queryString.indexOf("order by")).trim();
					} else {
						conditionStr = queryString.substring((queryString.indexOf(searchConditionStr) + searchConditionStr.length())).trim();
					}
					
				}
			}
			
			if(conditionStr != null && !conditionStr.isEmpty()) {
				if(conditionStr.contains(" and ") || conditionStr.contains(" or ")) {
					conditionStrArr = conditionStr.split(" and | or ");
				} else {
					conditionStrArr = new String[1];
					conditionStrArr[0] = conditionStr.trim();
				}
				
				if (conditionStrArr != null && conditionStrArr.length > 0) {
					restrictionList = new ArrayList<Restriction>();
					for (final String str : conditionStrArr) {
						String condition = null;
						if (str.contains("!=")) {
							condition = "!=";
						} else if (str.contains(">=")) {
							condition = ">=";
						} else if (str.contains("<=")) {
							condition = "<=";
						} else if (str.contains(">")) {
							condition = ">";
						} else if (str.contains("<")) {
							condition = "<";
						} else {
							condition = "=";
						}
						if (condition != null && !condition.isEmpty()) {
							final String propertyName = str.substring(0,
									str.indexOf(condition)).trim().replaceAll("'", "");
							final String propertValue = str.substring(str
									.indexOf(condition) + condition.length()).trim().replaceAll("'", "");
							Restriction restriction = new Restriction(propertyName,
									propertValue, condition);
							restrictionList.add(restriction);
						}
					}
				}
				
			}
			return restrictionList;
			}
			
	
	/*
	 * extract the logical operators(AND/OR) from the query, if at all it is
	 * present. For eg: select city,winner,team1,team2,player_of_match from
	 * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
	 * bangalore
	 * 
	 * the query mentioned above in the example should return a List of Strings
	 * containing [or,and]
	 */
		private List<String> getLogicalOperators(final String queryString) {
			String outputStr = null;
			final String startSearch = "where ";
			String[] logicalOperatorArr = null;
			List<String> logicalOperatorList = null;
			if(queryString != null && !queryString.isEmpty()) {
				if(queryString.contains(startSearch)) {
					if(queryString.contains("group by")) {
						outputStr = queryString.substring((queryString.indexOf(startSearch) + startSearch.length()), queryString.indexOf("group by")).trim();
					} else if(queryString.contains("order by")) {
						outputStr = queryString.substring((queryString.indexOf(startSearch) + startSearch.length()), queryString.indexOf("order by")).trim();
					} else {
						outputStr = queryString.substring((queryString.indexOf(startSearch) + startSearch.length())).trim();
					}
					
				}
			}
			
			if(outputStr != null && !outputStr.isEmpty()) {
				logicalOperatorList = new ArrayList<String>();
				if(outputStr.contains(" and ") || outputStr.contains(" or ")) {
					logicalOperatorArr = outputStr.split(" ");
					for(final String logicalConditionStr : logicalOperatorArr) {
						if(logicalConditionStr.equalsIgnoreCase("and") || logicalConditionStr.equalsIgnoreCase("or")) {
							logicalOperatorList.add(logicalConditionStr);
						}
					}
				} 
			}
			return logicalOperatorList;
			}
	
	/*
	 * extract the aggregate functions from the query. The presence of the aggregate
	 * functions can determined if we have either "min" or "max" or "sum" or "count"
	 * or "avg" followed by opening braces"(" after "select" clause in the query
	 * string. in case it is present, then we will have to extract the same. For
	 * each aggregate functions, we need to know the following: 
	 * 1. type of aggregate function(min/max/count/sum/avg) 
	 * 2. field on which the aggregate function is being applied
	 * 
	 * Please note that more than one aggregate function can be present in a query
	 * 
	 * 
	 */
		private List<AggregateFunction> getAggregateFunctions(final String queryString) {
			String[] aggregateStrArr = null;
			String aggregateStr = null;
			final String startStr = "select";
			final String endStr = "from";
			List<AggregateFunction> aggregateFunctionList = null;
			
			if(queryString != null && !queryString.isEmpty()) {
				if(queryString.contains("sum(") || queryString.contains("count(") || queryString.contains("min(") || queryString.contains("max(") || queryString.contains("avg(")) {
					aggregateStr = queryString.substring((queryString.indexOf(startStr) + startStr.length()), queryString.indexOf(endStr)).trim();
					if(aggregateStr != null && !aggregateStr.isEmpty()) {
						aggregateFunctionList = new ArrayList<AggregateFunction>();
						if(aggregateStr.contains(",")) {
							aggregateStrArr = aggregateStr.trim().split(",");	
							for(String singleElement : aggregateStrArr) {
								if(singleElement.startsWith("sum(") || singleElement.startsWith("count(") || singleElement.startsWith("min(") || singleElement.startsWith("max(") || singleElement.startsWith("avg(")) {
									final String function = singleElement.substring(0, singleElement.indexOf("(")).trim();
									final String field = singleElement.substring(singleElement.indexOf("(") + 1, singleElement.indexOf(")")).trim();
									AggregateFunction aggregateFunction = new AggregateFunction(field, function);
									aggregateFunctionList.add(aggregateFunction);
								}
							}
							
						} else {
							if(aggregateStr.trim().startsWith("sum(") || aggregateStr.trim().startsWith("count(") || aggregateStr.trim().startsWith("min(") || aggregateStr.trim().startsWith("max(") || aggregateStr.trim().startsWith("avg(")) {
								final String function = aggregateStr.substring(0, aggregateStr.indexOf("(")).trim();
								final String field = aggregateStr.substring(aggregateStr.indexOf("(") + 1, aggregateStr.indexOf(")")).trim();
								AggregateFunction aggregateFunction = new AggregateFunction(field, function);
								aggregateFunctionList.add(aggregateFunction);
							}
						}
					}
				}
			}
			return aggregateFunctionList;
			}
	
}
