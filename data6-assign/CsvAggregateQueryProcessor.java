package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.GroupedDataSet;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

/* This is the CsvAggregateQueryProcessor class used for evaluating queries with 
 * aggregate functions without group by clause*/
@SuppressWarnings("rawtypes")
public class CsvAggregateQueryProcessor implements QueryProcessingEngine {
	/*
	 * This method will take QueryParameter object as a parameter which contains
	 * the parsed query and will process and populate the ResultSet
	 */
	public HashMap getResultSet(QueryParameter queryParameter) {

		/*
		 * initialize BufferedReader to read from the file which is mentioned in
		 * QueryParameter. Consider Handling Exception related to file reading.
		 */

		/*
		 * read the first line which contains the header. Please note that the
		 * headers can contain spaces in between them. For eg: city, winner
		 */
		BufferedReader br = null;
		String line = null;
		String csvSplitBy = ",";
		String[] headers = null;
		GroupedDataSet groupedDataSet = new GroupedDataSet();
		List<Row> rowList = new ArrayList<Row>();
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
			 * read the next line which contains the first row of data. We are
			 * reading this line so that we can determine the data types of all
			 * the fields. Please note that ipl.csv file contains null value in
			 * the last column. If you do not consider this while splitting,
			 * this might cause exceptions later
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
			 * populate the header Map object from the header array. header map
			 * is having data type <String,Integer> to contain the header and
			 * it's index.
			 */
			HashMap<String, Integer> headerMap = new HashMap<String, Integer>();
			int rowIndex = 0;
			for (String headerName : headers) {
				headerMap.put(headerName, rowIndex);
				rowIndex++;
			}

			/*
			 * We have read the first line of text already and kept it in an
			 * array. Now, we can populate the dataTypeDefinition Map object.
			 * dataTypeDefinition map is having data type <Integer,String> to
			 * contain the index of the field and it's data type. To find the
			 * dataType by the field value, we will use getDataType() method of
			 * DataTypeDefinitions class
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
			 * once we have the header and dataTypeDefinitions maps populated,
			 * we can start reading from the first line. We will read one line
			 * at a time, then check whether the field values satisfy the
			 * conditions mentioned in the query,if yes, then we will add it to
			 * the resultSet. Otherwise, we will continue to read the next line.
			 * We will continue this till we have read till the last line of the
			 * CSV file.
			 */

			/*
			 * reset the buffered reader so that it can start reading from the
			 * first line
			 */

			/*
			 * skip the first line as it is already read earlier which contained
			 * the header
			 */

			/*
			 * read one line at a time from the CSV file till we have any lines
			 * left
			 */

			/*
			 * once we have read one line, we will split it into a String Array.
			 * This array will continue all the fields of the row. Please note
			 * that fields might contain spaces in between. Also, few fields
			 * might be empty.
			 */

			/*
			 * if there are where condition(s) in the query, test the row fields
			 * against those conditions to check whether the selected row
			 * satifies the conditions
			 */

			/*
			 * from QueryParameter object, read one condition at a time and
			 * evaluate the same. For evaluating the conditions, we will use
			 * evaluateExpressions() method of Filter class. Please note that
			 * evaluation of expression will be done differently based on the
			 * data type of the field. In case the query is having multiple
			 * conditions, you need to evaluate the overall expression i.e. if
			 * we have OR operator between two conditions, then the row will be
			 * selected if any of the condition is satisfied. However, in case
			 * of AND operator, the row will be selected only if both of them
			 * are satisfied.
			 */

			/*
			 * check for multiple conditions in where clause for eg: where
			 * salary>20000 and city=Bangalore for eg: where salary>20000 or
			 * city=Bangalore and dept!=Sales
			 */

			/*
			 * if the overall condition expression evaluates to true, then we
			 * need to check for the existence for aggregate functions in the
			 * Query Parameter. Please note that there can be more than one
			 * aggregate functions existing in a query. The dataSet generated
			 * after processing any aggregate function is completely different
			 * from a dataSet structure(which contains multiple rows of data).
			 * In case of queries containing aggregate functions, each row of
			 * the resultSet will contain the key(for e.g. 'count(city)') and
			 * it's aggregate value. Hence, we will use
			 * GroupedDataSet<String,Object> to store the same and not
			 * DataSet<Long,Row>. we will process all the five aggregate
			 * functions i.e. min, max, avg, sum, count.
			 */

			// return groupedDataSet object

			int rowCount = 1;
			String[] rowData = null;
			boolean filterStatus = false;
			Row row = null;
			try {
				br = new BufferedReader(new FileReader(
						queryParameter.getFileName()));
				while ((line = br.readLine()) != null) {
					if (rowCount == 1) {
						rowCount++;
						continue;
					} else {
						rowData = line.split(csvSplitBy, -1);

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
									row = getRecordListWithAggrFunc(
											queryParameter, headerMap, rowData);
									rowList.add(row);
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
									int counter = evaluationList.size() - 1;
									boolean overAllCondition = validateAllWhereConditions(
											logicalOperatorList,
											evaluationList, counter);
									if (overAllCondition) {
										row = getRecordListWithAggrFunc(
												queryParameter, headerMap,
												rowData);
										rowList.add(row);
									}

								}

							}

						} else {
							row = getRecordListWithAggrFunc(queryParameter,
									headerMap, rowData);
							rowList.add(row);
						}
						rowCount++;
					}
				}

				groupedDataSet = getGroupedDataSetWithAggregate(queryParameter,
						rowList);
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
		return groupedDataSet;
	}

	private GroupedDataSet getGroupedDataSetWithAggregate(
			QueryParameter queryParameter, List<Row> rowList) {
		GroupedDataSet groupedDataSet = new GroupedDataSet();
		if (rowList != null && !rowList.isEmpty()) {

			List<AggregateFunction> aggregateFunctionList = queryParameter
					.getAggregateFunctions();

			for (AggregateFunction aggrFunc : aggregateFunctionList) {
				if (aggrFunc.getFunction().equalsIgnoreCase("count")) {
					int t = 0;
					for (Row row1 : rowList) {
						String s = row1.get(aggrFunc.getField());
						if (s != null && !s.isEmpty()) {
							t++;
						}
					}
					groupedDataSet.put(
							aggrFunc.getFunction() + "(" + aggrFunc.getField()
									+ ")", t);
				} else if (aggrFunc.getFunction().equalsIgnoreCase("sum")) {
					double sum = 0;
					for (Row row1 : rowList) {
						String s = row1.get(aggrFunc.getField());
						if (s != null && !s.isEmpty()) {
							sum += Double.parseDouble(s);
						}
					}
					groupedDataSet.put(
							aggrFunc.getFunction() + "(" + aggrFunc.getField()
									+ ")", sum);
				} else if (aggrFunc.getFunction().equalsIgnoreCase("avg")) {
					double sum = 0;
					int t = 0;
					double avg = 0;
					for (Row row1 : rowList) {
						String s = row1.get(aggrFunc.getField());
						if (s != null && !s.isEmpty()) {
							sum += Double.parseDouble(s);
							t++;
						}
					}
					avg = sum / t;
					groupedDataSet.put(
							aggrFunc.getFunction() + "(" + aggrFunc.getField()
									+ ")", avg);
				} else if (aggrFunc.getFunction().equalsIgnoreCase("min")) {
					double min = 0;
					for (Row row1 : rowList) {
						String s = row1.get(aggrFunc.getField());
						if (s != null && !s.isEmpty()) {
							if (min > Double.parseDouble(s)) {
								min = Double.parseDouble(s);
							}

						}
					}
					groupedDataSet.put(
							aggrFunc.getFunction() + "(" + aggrFunc.getField()
									+ ")", min);

				} else if (aggrFunc.getFunction().equalsIgnoreCase("max")) {
					double max = 0;
					for (Row row1 : rowList) {
						String s = row1.get(aggrFunc.getField());
						if (s != null && !s.isEmpty()) {
							if (max < Double.parseDouble(s)) {
								max = Double.parseDouble(s);
							}
						}
					}
					groupedDataSet.put(
							aggrFunc.getFunction() + "(" + aggrFunc.getField()
									+ ")", max);

				}

			}

		}
		return groupedDataSet;
	}

	private boolean validateAllWhereConditions(
			List<String> logicalOperatorList, List<Boolean> evaluationList,
			int counter) {
		boolean temp = false;
		for (int i = logicalOperatorList.size() - 1; i >= 0; i--) {
			String logicalOperator = logicalOperatorList.get(i);
			if (counter == evaluationList.size() - 1) {
				if (logicalOperator.equalsIgnoreCase("AND")) {
					if (evaluationList.get(counter)
							&& evaluationList.get(counter - 1)) {
						temp = true;
					} else {
						temp = false;
					}
				} else if (logicalOperator.equalsIgnoreCase("OR")) {
					if (evaluationList.get(counter)
							|| evaluationList.get(counter - 1)) {
						temp = true;
					} else {
						temp = false;
					}
				}
			} else {
				if (logicalOperator.equalsIgnoreCase("AND")) {
					if (temp && evaluationList.get(counter - 1)) {
						temp = true;
					} else {
						temp = false;
					}
				} else if (logicalOperator.equalsIgnoreCase("OR")) {
					if (temp || evaluationList.get(counter - 1)) {
						temp = true;
					} else {
						temp = false;
					}
				}
			}
			counter--;
		}
		return temp;
	}

	private Row getRecordListWithAggrFunc(QueryParameter queryParameter,
			HashMap<String, Integer> headerMap, String[] rowData) {
		Row row = new Row();
		if (queryParameter.getAggregateFunctions() != null
				&& !queryParameter.getAggregateFunctions().isEmpty()) {
			List<AggregateFunction> aggregateFunctionList = queryParameter
					.getAggregateFunctions();
			for (AggregateFunction aggrFunc : aggregateFunctionList) {
				row.put(aggrFunc.getField(),
						rowData[headerMap.get(aggrFunc.getField())]);
			}
		}
		return row;
	}

}
