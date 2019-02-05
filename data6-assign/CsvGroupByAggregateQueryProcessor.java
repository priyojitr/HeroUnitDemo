package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.GroupedDataSet;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

/* This is the CsvGroupByAggregateQueryProcessor class used for evaluating queries with 
 * aggregate functions and group by clause*/
@SuppressWarnings("rawtypes")
public class CsvGroupByAggregateQueryProcessor implements QueryProcessingEngine {
	/*
	 * This method will take QueryParameter object as a parameter which contains the
	 * parsed query and will process and populate the ResultSet
	 */
	public HashMap getResultSet(QueryParameter queryParameter) {

		/*
		 * initialize BufferedReader to read from the file which is mentioned in
		 * QueryParameter. Consider Handling Exception related to file reading.
		 */

		/*
		 * read the first line which contains the header. Please note that the headers
		 * can contain spaces in between them. For eg: city, winner
		 */
		BufferedReader br = null;
		String line = null;
		String csvSplitBy = ",";
		String[] headers = null;
		DataSet dataSet = new DataSet();
		GroupedDataSet groupedDataSet = new GroupedDataSet();
		GroupedDataSet finalGroupedDataSet = new GroupedDataSet();
		try {
			try {

				br = new BufferedReader(new FileReader(
						queryParameter.getFileName()));
				while ((line = br.readLine()) != null) {

					headers = line.split(csvSplitBy);
					break;
				}

			} catch (FileNotFoundException e) {
				System.out.println("File Not Found Exception occurred");
			} catch (IOException e) {
				System.out.println("IO Exception occurred");
			}

		/*
		 * read the next line which contains the first row of data. We are reading this
		 * line so that we can determine the data types of all the fields. Please note
		 * that ipl.csv file contains null value in the last column. If you do not
		 * consider this while splitting, this might cause exceptions later
		 */
			int count = 0;
			String[] dataValues = null;
			try {
				br = new BufferedReader(new FileReader(
						queryParameter.getFileName()));
				while ((line = br.readLine()) != null) {
					if (count == 1) {
						dataValues = line.split(csvSplitBy, -1);
						break;
					}
					count++;
				}
			} catch (FileNotFoundException e) {
				System.out.println("File Not Found Exception occurred");
			} catch (IOException e) {
				System.out.println("IO Exception occurred");
			}

		/*
		 * populate the header Map object from the header array. header map is having
		 * data type <String,Integer> to contain the header and it's index.
		 */
			HashMap<String, Integer> headerMap = new HashMap<String, Integer>();
			int rowIndex = 0;
			for (String headerName : headers) {
				headerMap.put(headerName, rowIndex);
				rowIndex++;
			}

		/*
		 * We have read the first line of text already and kept it in an array. Now, we
		 * can populate the dataTypeDefinition Map object. dataTypeDefinition map is
		 * having data type <Integer,String> to contain the index of the field and it's
		 * data type. To find the dataType by the field value, we will use getDataType()
		 * method of DataTypeDefinitions class
		 */
			RowDataTypeDefinitions rowDataTypeDef = new RowDataTypeDefinitions();
			int dataIndex = 0;
			for (String dataValue : dataValues) {
				String dataType = (String) DataTypeDefinitions
						.getDataType(dataValue);
				rowDataTypeDef.put(dataIndex, dataType);
				dataIndex++;
			}


		/*
		 * once we have the header and dataTypeDefinitions maps populated, we can start
		 * reading from the first line. We will read one line at a time, then check
		 * whether the field values satisfy the conditions mentioned in the query,if
		 * yes, then we will add it to the resultSet. Otherwise, we will continue to
		 * read the next line. We will continue this till we have read till the last
		 * line of the CSV file.
		 */

		/* reset the buffered reader so that it can start reading from the first line */

		/*
		 * skip the first line as it is already read earlier which contained the header
		 */

		/* read one line at a time from the CSV file till we have any lines left */

		/*
		 * once we have read one line, we will split it into a String Array. This array
		 * will continue all the fields of the row. Please note that fields might
		 * contain spaces in between. Also, few fields might be empty.
		 */

		/*
		 * if there are where condition(s) in the query, test the row fields against
		 * those conditions to check whether the selected row satifies the conditions
		 */

		/*
		 * from QueryParameter object, read one condition at a time and evaluate the
		 * same. For evaluating the conditions, we will use evaluateExpressions() method
		 * of Filter class. Please note that evaluation of expression will be done
		 * differently based on the data type of the field. In case the query is having
		 * multiple conditions, you need to evaluate the overall expression i.e. if we
		 * have OR operator between two conditions, then the row will be selected if any
		 * of the condition is satisfied. However, in case of AND operator, the row will
		 * be selected only if both of them are satisfied.
		 */

		/*
		 * check for multiple conditions in where clause for eg: where salary>20000 and
		 * city=Bangalore for eg: where salary>20000 or city=Bangalore and dept!=Sales
		 */

		/*
		 * if the overall condition expression evaluates to true, then we need to check
		 * for the existence for group by clause in the Query Parameter. The dataSet
		 * generated after processing a group by with aggregate clause is completely
		 * different from a dataSet structure(which contains multiple rows of data). In
		 * case of queries containing group by clause and aggregate functions, the
		 * resultSet will contain multiple dataSets, each of which will be assigned to
		 * the group by column value i.e. for all unique values of the group by column,
		 * aggregates will have to be calculated. Hence, we will use
		 * GroupedDataSet<String,Object> to store the same and not DataSet<Long,Row>.
		 * Please note we will process queries containing one group by column only for
		 * this example.
		 */

		// return groupedDataSet object

			String[] rowData = null;
			Long rowCount = new Long(0);
			boolean filterStatus = false;
			Row row = null;
			try {
				br = new BufferedReader(new FileReader(
						queryParameter.getFileName()));
				while ((line = br.readLine()) != null) {
					if (rowCount == 0) {
						rowCount++;
						continue;
					} else {
						rowData = line.split(csvSplitBy, -1);
						row = new Row();

						if (queryParameter.getRestrictions() != null
								&& !queryParameter.getRestrictions().isEmpty()) {

							if (queryParameter.getRestrictions().size() == 1) {
								Restriction restriction = queryParameter
										.getRestrictions().get(0);
								filterStatus = Filter.evaluateExpression(
										restriction, rowData[headerMap
												.get(restriction
														.getPropertyName())],
										rowDataTypeDef.get(headerMap
												.get(restriction
														.getPropertyName())));
								if (filterStatus) {
									for (String fieldName : headerMap.keySet()) {
										row.put(fieldName,
												rowData[headerMap.get(fieldName)]);
										dataSet.put(rowCount, row);
									}
								}

							} else {

								List<Restriction> restrictionList = queryParameter
										.getRestrictions();
								List<String> logicalOperatorList = queryParameter
										.getLogicalOperators();
								List<Boolean> evaluationList = new ArrayList<Boolean>();
								for (Restriction restriction : restrictionList) {
									boolean status = Filter
											.evaluateExpression(
													restriction,
													rowData[headerMap.get(restriction
															.getPropertyName())],
													rowDataTypeDef.get(headerMap.get(restriction
															.getPropertyName())));
									evaluationList.add(status);
								}

								if (logicalOperatorList != null
										&& !logicalOperatorList.isEmpty()) {
									boolean overAllCondition = validateAllLogicalOperators(
											logicalOperatorList,
											evaluationList);
									if (overAllCondition) {
											for (String fieldName : headerMap.keySet()) {
												row.put(fieldName,
														rowData[headerMap.get(fieldName)]);
												dataSet.put(rowCount, row);
											}
									}

								}

							}

						} else {
							for (String fieldName : headerMap.keySet()) {
								row.put(fieldName,
										rowData[headerMap.get(fieldName)]);
								dataSet.put(rowCount, row);
							}
						}
						rowCount++;
					}
				}
				
				if(dataSet != null && !dataSet.isEmpty()) {
					List<String> groupByFields = queryParameter.getGroupByFields();
					List<AggregateFunction> aggrFuncList = queryParameter.getAggregateFunctions();
					if((groupByFields != null && !groupByFields.isEmpty()) && (aggrFuncList != null && !aggrFuncList.isEmpty())) {
						groupedDataSet = getGroupedDataSetWithGroupBy(dataSet,
									groupByFields);
						finalGroupedDataSet = getFinalGroupedDataSetWithAggrFunc(queryParameter,
									groupedDataSet);
						}
					}
			} catch (FileNotFoundException e) {
				System.out.println("File Not Found Exception occurred");
			} catch (IOException e) {
				System.out.println("IO Exception occurred");
			}
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}
		}
		return finalGroupedDataSet;
	}

	private boolean validateAllLogicalOperators(
			List<String> logicalOperatorList, List<Boolean> evaluationList) {
		int counter = 0;
		boolean temp = false;
		for (int i = 0; i < logicalOperatorList.size(); i++) {
			String logicalOperator = logicalOperatorList
					.get(i);
			if (counter == 0) {
				if (logicalOperator
						.equalsIgnoreCase("AND")) {
					if (evaluationList.get(counter)
							&& evaluationList
									.get(counter + 1)) {
						temp = true;
					} else {
						temp = false;
					}
				} else if (logicalOperator
						.equalsIgnoreCase("OR")) {
					if (evaluationList.get(counter)
							|| evaluationList
									.get(counter + 1)) {
						temp = true;
					} else {
						temp = false;
					}
				}
			} else {
				if (logicalOperator
						.equalsIgnoreCase("AND")) {
					if (temp
							&& evaluationList
									.get(counter + 1)) {
						temp = true;
					} else {
						temp = false;
					}
				} else if (logicalOperator
						.equalsIgnoreCase("OR")) {
					if (temp
							|| evaluationList
									.get(counter + 1)) {
						temp = true;
					} else {
						temp = false;
					}
				}
			}
			counter++;
			
		}
		return temp;
	}

	private GroupedDataSet getFinalGroupedDataSetWithAggrFunc(
			QueryParameter queryParameter, GroupedDataSet groupedDataSet) {
		GroupedDataSet finalGroupedDataSet = new GroupedDataSet();
		for(Entry<String, Object> groupedDataEntry : groupedDataSet.entrySet()) {
			String groupedKey = groupedDataEntry.getKey();
		List<Row> rowList = (List<Row>) groupedDataEntry.getValue();

		
		List<AggregateFunction> aggregateFunctionList = queryParameter.getAggregateFunctions();
		
		for(AggregateFunction aggrFunc : aggregateFunctionList) {
			if(aggrFunc.getFunction().equalsIgnoreCase("count")) {
				int t = 0;
				for(Row dataRow : rowList) {
					if("*".equals(aggrFunc.getField())) {
						t++;
					} else {
					String s = dataRow.get(aggrFunc.getField());
					if(s != null && !s.isEmpty()) {
						t++;
					}
					}
				}
				finalGroupedDataSet.put(groupedKey, t);
			} else if(aggrFunc.getFunction().equalsIgnoreCase("sum")) {
				Integer sum = 0;
				for(Row dataRow : rowList) {
					String s = dataRow.get(aggrFunc.getField());
					if(s != null && !s.isEmpty()) {
						sum += Integer.parseInt(s);
					}
				}
				finalGroupedDataSet.put(groupedKey, sum);
			} else if(aggrFunc.getFunction().equalsIgnoreCase("avg")) {
				double sum = 0;
				int t = 0;
				double avg = 0;
				for(Row dataRow : rowList) {
					String s = dataRow.get(aggrFunc.getField());
					if(s != null && !s.isEmpty()) {
						sum += Double.parseDouble(s);
						t++;
					}
				}
				avg = sum/t;
				finalGroupedDataSet.put(groupedKey, avg);
			} else if(aggrFunc.getFunction().equalsIgnoreCase("min")) {
				Integer min = 0;
				int minCounter = 0;
				for(Row dataRow : rowList) {
					String s = dataRow.get(aggrFunc.getField());
					if(s != null && !s.isEmpty()) {
						if(minCounter == 0) {
							min = Integer.parseInt(s);
						} else if(minCounter > 0) {
						if(min > Integer.parseInt(s)) {
							min = Integer.parseInt(s);
						}
						}
						minCounter++;
					}
					
				}
				finalGroupedDataSet.put(groupedKey, min);
				
			} else if(aggrFunc.getFunction().equalsIgnoreCase("max")) {
				Integer max = 0;
				for(Row dataRow : rowList) {
					String s = dataRow.get(aggrFunc.getField());
					if(s != null && !s.isEmpty()) {
						if(max < Integer.parseInt(s)) {
							max = Integer.parseInt(s);
						}
						
					}
				}
				finalGroupedDataSet.put(groupedKey, max);
				
			}
			
		}
			
		}
		return finalGroupedDataSet;
	}

	private GroupedDataSet getGroupedDataSetWithGroupBy(DataSet dataSet,
			List<String> groupByFields) {
		GroupedDataSet groupedDataSet = new GroupedDataSet();
		for (String groupByField : groupByFields) {
			for (Entry<Long, Row> entry : dataSet.entrySet()) {
				Long key = entry.getKey();
				Row value = entry.getValue();
				List<Row> groupedDataRow = null;
				if (groupedDataSet == null || groupedDataSet.isEmpty()) {
					groupedDataRow = new ArrayList<Row>();
					groupedDataRow.add(value);
				} else if (value.get(groupByField) != null
						&& groupedDataSet.containsKey(value.get(groupByField))) {
					groupedDataRow = (List<Row>) groupedDataSet.get(value
							.get(groupByField));
					groupedDataRow.add(value);
				} else if (value.get(groupByField) != null
						&& !groupedDataSet.containsKey(value.get(groupByField))) {
					groupedDataRow = new ArrayList<Row>();
					groupedDataRow.add(value);
				}
				groupedDataSet.put(value.get(groupByField), groupedDataRow);
			}
		}
		return groupedDataSet;
	}

}
