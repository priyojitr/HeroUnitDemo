package com.stackroute.datamunger.query;

import java.util.Date;

import com.stackroute.datamunger.query.parser.Restriction;

//This class contains methods to evaluate expressions
public class Filter {

	/*
	 * the evaluateExpression() method of this class is responsible for evaluating
	 * the expressions mentioned in the query. It has to be noted that the process
	 * of evaluating expressions will be different for different data types. there
	 * are 6 operators that can exist within a query i.e. >=,<=,<,>,!=,= This method
	 * should be able to evaluate all of them. Note: while evaluating string
	 * expressions, please handle uppercase and lowercase
	 * 
	 */

	public static boolean evaluateExpression(Restriction restriction, String rowData,
			String dataType) {
		boolean status = false;
		if (restriction.getCondition().equals("=")) {
			status = equalTo(restriction.getPropertyValue(), rowData, dataType);
		} else if (restriction.getCondition().equals("!=")) {
			status = notEqualTo(restriction.getPropertyValue(), rowData,
					dataType);
		} else if (restriction.getCondition().equals(">")) {
			status = greaterThan(restriction.getPropertyValue(), rowData,
					dataType);
		} else if (restriction.getCondition().equals(">=")) {
			status = greaterThanOrEqualTo(restriction.getPropertyValue(),
					rowData, dataType);
		} else if (restriction.getCondition().equals("<")) {
			status = lessThan(restriction.getPropertyValue(), rowData, dataType);
		} else if (restriction.getCondition().equals("<=")) {
			status = lessThanOrEqualTo(restriction.getPropertyValue(), rowData,
					dataType);
		}

		return status;
	}

	// Method containing implementation of equalTo operator
	private static boolean equalTo(String restrictionVal, String rowData,
			String dataType) {
		boolean status = false;
		if (dataType.equals("java.lang.Integer")) {
			status = (Integer.parseInt(restrictionVal) == Integer
					.parseInt(rowData)) ? true : false;
		} else if (dataType.equals("java.lang.Double")) {
			status = (Double.parseDouble(restrictionVal) == Double
					.parseDouble(rowData)) ? true : false;
		} else if (dataType.equals("java.util.Date")) {
			status = (Date.parse(restrictionVal) == Date.parse(rowData)) ? true
					: false;
		} else {
			status = (rowData.toLowerCase()
					.equals(restrictionVal.toLowerCase())) ? true : false;
		}
		return status;
	}

	// Method containing implementation of notEqualTo operator
	private static boolean notEqualTo(String restrictionVal, String rowData,
			String dataType) {
		boolean status = false;
		if (dataType.equals("java.lang.Integer")) {
			status = (Integer.parseInt(restrictionVal) != Integer
					.parseInt(rowData)) ? true : false;
		} else if (dataType.equals("java.lang.Double")) {
			status = (Double.parseDouble(restrictionVal) != Double
					.parseDouble(rowData)) ? true : false;
		} else if (dataType.equals("java.util.Date")) {
			status = (Date.parse(restrictionVal) != Date.parse(rowData)) ? true
					: false;
		} else {
			status = (!rowData.toLowerCase().equals(
					restrictionVal.toLowerCase())) ? true : false;
		}
		return status;
	}

	// Method containing implementation of greaterThan operator

	private static boolean greaterThan(String restrictionVal, String rowData,
			String dataType) {
		boolean status = false;
		if (dataType.equals("java.lang.Integer")) {
			status = (Integer.parseInt(rowData) > Integer
					.parseInt(restrictionVal)) ? true : false;
		} else if (dataType.equals("java.lang.Double")) {
			status = (Double.parseDouble(rowData) > Double
					.parseDouble(restrictionVal)) ? true : false;
		} else if (dataType.equals("java.util.Date")) {
			status = (Date.parse(rowData) > Date.parse(restrictionVal)) ? true
					: false;
		} else {
			status = false;
		}
		return status;
	}

	// Method containing implementation of greaterThanOrEqualTo operator
	private static boolean greaterThanOrEqualTo(String restrictionVal,
			String rowData, String dataType) {
		boolean status = false;
		if (dataType.equals("java.lang.Integer")) {
			status = (Integer.parseInt(rowData) >= Integer
					.parseInt(restrictionVal)) ? true : false;
		} else if (dataType.equals("java.lang.Double")) {
			status = (Double.parseDouble(rowData) >= Double
					.parseDouble(restrictionVal)) ? true : false;
		} else if (dataType.equals("java.util.Date")) {
			status = (Date.parse(rowData) >= Date.parse(restrictionVal)) ? true
					: false;
		} else {
			status = false;
		}
		return status;
	}

	// Method containing implementation of lessThan operator

	private static boolean lessThan(String restrictionVal, String rowData,
			String dataType) {
		boolean status = false;
		if (dataType.equals("java.lang.Integer")) {
			status = (Integer.parseInt(rowData) < Integer
					.parseInt(restrictionVal)) ? true : false;
		} else if (dataType.equals("java.lang.Double")) {
			status = (Double.parseDouble(rowData) < Double
					.parseDouble(restrictionVal)) ? true : false;
		} else if (dataType.equals("java.util.Date")) {
			status = (Date.parse(rowData) < Date.parse(restrictionVal)) ? true
					: false;
		} else {
			status = false;
		}
		return status;
	}

	// Method containing implementation of lessThanOrEqualTo operator
	private static boolean lessThanOrEqualTo(String restrictionVal,
			String rowData, String dataType) {
		boolean status = false;
		if (dataType.equals("java.lang.Integer")) {
			status = (Integer.parseInt(rowData) <= Integer
					.parseInt(restrictionVal)) ? true : false;
		} else if (dataType.equals("java.lang.Double")) {
			status = (Double.parseDouble(rowData) <= Double
					.parseDouble(restrictionVal)) ? true : false;
		} else if (dataType.equals("java.util.Date")) {
			status = (Date.parse(rowData) <= Date.parse(restrictionVal)) ? true
					: false;
		} else {
			status = false;
		}
		return status;
	}

}
