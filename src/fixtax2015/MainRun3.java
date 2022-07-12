package fixtax2015;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class MainRun3 {

	public static String targedb = "local";

	public static void main(String[] args) {
		new StringBuffer();
		Float targetamountadjust = 0.00f;
		Date adjustdatefrom = null;
		Date adjustdateto = null;
		Map<Integer, Float> invoiceList = new LinkedHashMap<Integer, Float>();
		List<String> customerList = new ArrayList<String>();
		String sqlAdjust = "SELECT * from taxadjust order by adjustdatefrom";

		Float additionaladjust = 0.00f;

		Map<Integer, Integer> additionalinvoiceList = new LinkedHashMap<Integer, Integer>();

		System.out.println("\n");
		System.out.println("-------Start------");
		try {
			Connection connection = null;
			BvasConFatory bvasConFactory = new BvasConFatory(targedb);
			connection = bvasConFactory.getConnection();

			PreparedStatement pstmtAdjust = null;
			ResultSet rsAdjust = null;

			pstmtAdjust = connection.prepareStatement(sqlAdjust);

			rsAdjust = pstmtAdjust.executeQuery(sqlAdjust);
			while (rsAdjust.next()) {
				targetamountadjust = rsAdjust.getFloat("adjustamount");
				adjustdatefrom = rsAdjust.getDate("adjustdatefrom");
				adjustdateto = rsAdjust.getDate("adjustdateto");
				additionaladjust = rsAdjust.getFloat("additionaladjust");
				
				System.out.println("STARTDATE:" + " " + adjustdatefrom + " ENDDATE:" + adjustdateto + "  TARGET:"
						+ targetamountadjust + "  additional:" + additionaladjust);
				System.out.println("\n");
				
				invoiceList.clear();
				invoiceList = getAllMatchedInvoices(adjustdatefrom,
						adjustdateto);
				
				customerList = getAllMatchingCustomer(adjustdatefrom,
						adjustdateto);
				processEntries(invoiceList,customerList, targetamountadjust);
				
				additionalinvoiceList.clear();
				additionalinvoiceList = getAllMatchedAdditionalInvoices(adjustdatefrom, adjustdateto);
				if (additionaladjust > 0) {
					processAdditionalAdjustPlus(additionalinvoiceList, additionaladjust, true);
				} else {
					processAdditionalAdjustMinus(additionalinvoiceList, additionaladjust, false);
				}
				
			}
			
			System.out.println("\n");

			rsAdjust.close();
			pstmtAdjust.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("-------End------");
	}// end main
	
	private static Map<Integer, Float> getAllMatchedInvoices(Date adjustdatefrom, Date adjustdateto) {
		Map<Integer, Float> invoiceList = new LinkedHashMap<Integer, Float>();
		String sqlMatchInvoices= "SELECT i.invoicenumber , i.tax FROM invoice i " +
		" WHERE i.orderdate BETWEEN  ? AND ?  " +
		" AND ( (i.CustomerID IN (SELECT customerid FROM customer WHERE TaxID = 'N')) OR (customerid='1111111111') OR ('2222222222') )  " + 
		" AND i.tax > 0 AND i.invoicenumber NOT IN (SELECT returnedinvoice FROM returned)  ORDER BY  tax desc ";

		try {
			Connection connection = null;
			BvasConFatory bvasConFactory = new BvasConFatory(targedb);
			connection = bvasConFactory.getConnection();

			PreparedStatement pstmtAdjust = null;
			ResultSet rsAdjust = null;

			pstmtAdjust = connection.prepareStatement(sqlMatchInvoices);
			pstmtAdjust.setDate(1, adjustdatefrom);
			pstmtAdjust.setDate(2, adjustdateto);
			// System.out.println(sqlMatchInvoices);
			rsAdjust = pstmtAdjust.executeQuery();
			while (rsAdjust.next()) {
				invoiceList.put(rsAdjust.getInt("invoicenumber"), rsAdjust.getFloat("tax"));
				
			}

			rsAdjust.close();
			pstmtAdjust.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return invoiceList;
	}
	
	private static List<String> getAllMatchingCustomer(Date adjustdatefrom, Date adjustdateto) {

		List<String> customerList = new LinkedList<String>();
		String sqlMatchInvoices = "SELECT i.customerid  FROM invoice i " + " WHERE i.orderdate BETWEEN  ? AND ? "
				+ " AND i.CustomerID IN (SELECT customerid FROM customer WHERE TaxID = 'Y') ";

		try {
			Connection connection = null;
			BvasConFatory bvasConFactory = new BvasConFatory(targedb);
			connection = bvasConFactory.getConnection();

			PreparedStatement pstmtAdjust = null;
			ResultSet rsAdjust = null;

			pstmtAdjust = connection.prepareStatement(sqlMatchInvoices);
			pstmtAdjust.setDate(1, adjustdatefrom);
			pstmtAdjust.setDate(2, adjustdateto);

			rsAdjust = pstmtAdjust.executeQuery();
			while (rsAdjust.next()) {
				customerList.add(rsAdjust.getString("customerid"));
			}

			rsAdjust.close();
			pstmtAdjust.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return customerList;

	}
	
	private static void processEntries(Map<Integer, Float> invoiceList, List<String> customerList,
			Float targetamount) {
		Float finalamountadjust = 0.00f;
		Connection connection = null;
		PreparedStatement pstmtTaxer = null;
		int n = 0;
		String updateInvoiceSQL = "UPDATE invoice SET   Tax = 0.00, CustomerID = ? WHERE invoicenumber = ?";

		try {
			Random rand = new Random();
			BvasConFatory bvasConFactory = new BvasConFatory(targedb);
			connection = bvasConFactory.getConnection();
			for (Map.Entry<Integer, Float> entry : invoiceList.entrySet()) {
				int num = rand.nextInt(customerList.size() - 1);
				if (finalamountadjust < targetamount) {
					pstmtTaxer = connection.prepareStatement(updateInvoiceSQL);
					pstmtTaxer.setString(1, customerList.get(num));	
					pstmtTaxer.setInt(2, entry.getKey());
					finalamountadjust = finalamountadjust + entry.getValue();
					//System.out.println( entry.getKey() + "\t" + entry.getValue());
					pstmtTaxer.executeUpdate();
					n++;
				}else{
					System.out.println("finalamountadjust:" + finalamountadjust + " targetamount:" + targetamount);
					System.out.println("**" + n);
					finalamountadjust =  0.00f;
					break;
				}

			} // for loop
			pstmtTaxer.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	private static Map<Integer, Integer> getAllMatchedAdditionalInvoices(Date adjustdatefrom, Date adjustdateto) {
		Map<Integer, Integer> additionalinvoiceList = new LinkedHashMap<Integer, Integer>();
		String sqlMatchInvoices = "SELECT i.invoicenumber , COUNT(id.partnumber) cnt FROM invoice i, invoicedetails id "
				+ " WHERE i.InvoiceNumber = id.InvoiceNumber " + " AND i.orderdate BETWEEN  ? AND ? "
				+ " AND ( (i.CustomerID IN (SELECT customerid FROM customer WHERE TaxID = 'N' )) OR (customerid='1111111111') OR ('2222222222') )  "
				+ " and i.tax = 0"
				+ " and i.returnedinvoice= 0 GROUP BY i.invoicenumber  having cnt >= 2 ORDER BY  i.invoicetotal ";

		try {
			Connection connection = null;
			BvasConFatory bvasConFactory = new BvasConFatory(targedb);
			connection = bvasConFactory.getConnection();

			PreparedStatement pstmtAdjust = null;
			ResultSet rsAdjust = null;

			pstmtAdjust = connection.prepareStatement(sqlMatchInvoices);
			pstmtAdjust.setDate(1, adjustdatefrom);
			pstmtAdjust.setDate(2, adjustdateto);
			// System.out.println(sqlMatchInvoices);
			rsAdjust = pstmtAdjust.executeQuery();
			while (rsAdjust.next()) {
				additionalinvoiceList.put(rsAdjust.getInt("invoicenumber"), rsAdjust.getInt("cnt") - 1);
				// System.out.println(rsAdjust.getInt("invoicenumber"));
			}

			rsAdjust.close();
			pstmtAdjust.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return additionalinvoiceList;
	}
	
	private static void updateInvoice(List<AdditionalAdjustInvoice> invoicelist, boolean index) {
		String updateInvoiceSQL = "";
		if (index) {

			updateInvoiceSQL = "UPDATE invoice SET  invoicetotal = invoicetotal + ? WHERE invoicenumber = ?";
		} else {
			updateInvoiceSQL = "UPDATE invoice SET  invoicetotal = invoicetotal - ? WHERE invoicenumber = ?";
		}

		Connection connection = null;
		PreparedStatement pstmtUpdateInvoice = null;
		BvasConFatory bvasConFactory = new BvasConFatory(targedb);
		try {

			connection = bvasConFactory.getConnection();
			for (AdditionalAdjustInvoice ainv : invoicelist) {

				pstmtUpdateInvoice = connection.prepareStatement(updateInvoiceSQL);
				pstmtUpdateInvoice.setFloat(1, ainv.getAdjustamount());
				pstmtUpdateInvoice.setInt(2, ainv.getInvoicenumber());
				pstmtUpdateInvoice.executeUpdate();

			}

			pstmtUpdateInvoice.close();

			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private static void processAdditionalAdjustMinus(Map<Integer, Integer> additionalinvoiceList,
			Float additionaladjust, boolean indice) {
		Float finaladditionaladjust = 0.00f;
		Float invoiceadditionaladjust = 0.00f;
		Connection connection = null;
		PreparedStatement pstmtAdditional = null;
		ResultSet rsAdditional = null;
		String sqlAdditional = "SELECT i.invoicenumber , id.partnumber, id.SoldPrice,  round((id.SoldPrice * 0.25),2) as addamount"
				+ " FROM invoice i , invoicedetails id WHERE i.InvoiceNumber = id.InvoiceNumber AND i.invoicenumber = ? ORDER BY ID.SoldPrice   LIMIT ?;";
		List<AdditionalAdjustInvoiceDetails> invoicedetailslist = new ArrayList<AdditionalAdjustInvoiceDetails>();
		List<AdditionalAdjustInvoice> invoicelist = new ArrayList<AdditionalAdjustInvoice>();
		try {

			BvasConFatory bvasConFactory = new BvasConFatory(targedb);
			connection = bvasConFactory.getConnection();
			additionaladjust = additionaladjust * -1;
			for (Map.Entry<Integer, Integer> entry : additionalinvoiceList.entrySet()) {
				invoiceadditionaladjust = 0.00f;
				AdditionalAdjustInvoice ia = new AdditionalAdjustInvoice();
				if (finaladditionaladjust < additionaladjust) {
					pstmtAdditional = connection.prepareStatement(sqlAdditional);
					pstmtAdditional.setInt(1, entry.getKey());
					pstmtAdditional.setInt(2, entry.getValue());
					rsAdditional = pstmtAdditional.executeQuery();
					while (rsAdditional.next()) {

						AdditionalAdjustInvoiceDetails ida = new AdditionalAdjustInvoiceDetails();
						ida.setInvoicenumber(entry.getKey());
						ida.setPartnumber(rsAdditional.getString("partnumber"));
						ida.setAddddetailsamount(rsAdditional.getFloat("addamount"));
						invoicedetailslist.add(ida);
						invoiceadditionaladjust = invoiceadditionaladjust + rsAdditional.getFloat("addamount");

						if (finaladditionaladjust > additionaladjust) {
							System.out.println(finaladditionaladjust + " ^ " + additionaladjust);
							break;
						}

					}

				} else {
					System.out.println(finaladditionaladjust + " ^ " + additionaladjust);
					break;
				}
				ia.setInvoicenumber(entry.getKey());
				ia.setAdjustamount(invoiceadditionaladjust);
				invoicelist.add(ia);
				finaladditionaladjust = finaladditionaladjust + invoiceadditionaladjust;
			}
			updateInvoice(invoicelist, indice);
			updateInvoiceDetails(invoicedetailslist, indice);
			if (rsAdditional != null) {
				rsAdditional.close();
			}
			if (pstmtAdditional != null) {
				pstmtAdditional.close();
			}
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	private static void updateInvoiceDetails(List<AdditionalAdjustInvoiceDetails> invoicedetailslist, boolean index) {

		String updateInvoiceDetailsSQL = "";
		if (index) {

			updateInvoiceDetailsSQL = "UPDATE invoicedetails SET soldprice = soldprice + ?  WHERE invoicenumber = ? AND PartNumber = ?";
		} else {
			updateInvoiceDetailsSQL = "UPDATE invoicedetails SET soldprice = soldprice - ?  WHERE invoicenumber = ? AND PartNumber = ?";
		}

		Connection connection = null;

		PreparedStatement pstmtupdateInvoiceDetails = null;
		BvasConFatory bvasConFactory = new BvasConFatory(targedb);
		try {

			connection = bvasConFactory.getConnection();
			for (AdditionalAdjustInvoiceDetails taxmatrix : invoicedetailslist) {

				pstmtupdateInvoiceDetails = connection.prepareStatement(updateInvoiceDetailsSQL);
				pstmtupdateInvoiceDetails.setFloat(1, taxmatrix.getAddddetailsamount());
				pstmtupdateInvoiceDetails.setInt(2, taxmatrix.getInvoicenumber());
				pstmtupdateInvoiceDetails.setString(3, taxmatrix.getPartnumber());
				pstmtupdateInvoiceDetails.executeUpdate();
			}
			pstmtupdateInvoiceDetails.close();
			pstmtupdateInvoiceDetails.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	private static void processAdditionalAdjustPlus(Map<Integer, Integer> additionalinvoiceList, Float additionaladjust,
			boolean indice) {
		Float finaladditionaladjust = 0.00f;
		Float invoiceadditionaladjust = 0.00f;
		Connection connection = null;
		PreparedStatement pstmtAdditional = null;
		ResultSet rsAdditional = null;
		String sqlAdditional = "SELECT i.invoicenumber , id.partnumber, id.SoldPrice,  round((id.SoldPrice * 0.05),2) as addamount"
				+ " FROM invoice i , invoicedetails id WHERE i.InvoiceNumber = id.InvoiceNumber AND i.invoicenumber = ? ORDER BY ID.SoldPrice desc  LIMIT ?;";
		List<AdditionalAdjustInvoiceDetails> invoicedetailslist = new ArrayList<AdditionalAdjustInvoiceDetails>();
		List<AdditionalAdjustInvoice> invoicelist = new ArrayList<AdditionalAdjustInvoice>();
		try {

			BvasConFatory bvasConFactory = new BvasConFatory(targedb);
			connection = bvasConFactory.getConnection();

			for (Map.Entry<Integer, Integer> entry : additionalinvoiceList.entrySet()) {
				invoiceadditionaladjust = 0.00f;
				AdditionalAdjustInvoice ia = new AdditionalAdjustInvoice();
				if (finaladditionaladjust < additionaladjust) {
					pstmtAdditional = connection.prepareStatement(sqlAdditional);
					pstmtAdditional.setInt(1, entry.getKey());
					pstmtAdditional.setInt(2, entry.getValue());
					rsAdditional = pstmtAdditional.executeQuery();
					while (rsAdditional.next()) {

						AdditionalAdjustInvoiceDetails ida = new AdditionalAdjustInvoiceDetails();
						ida.setInvoicenumber(entry.getKey());
						ida.setPartnumber(rsAdditional.getString("partnumber"));
						ida.setAddddetailsamount(rsAdditional.getFloat("addamount"));
						invoicedetailslist.add(ida);
						invoiceadditionaladjust = invoiceadditionaladjust + rsAdditional.getFloat("addamount");

						if (finaladditionaladjust > additionaladjust) {
							System.out.println(finaladditionaladjust + " ^ " + additionaladjust);
							break;
						}

					}

				} else {
					System.out.println(finaladditionaladjust + " ^ " + additionaladjust);
					break;
				}
				ia.setInvoicenumber(entry.getKey());
				ia.setAdjustamount(invoiceadditionaladjust);
				invoicelist.add(ia);
				finaladditionaladjust = finaladditionaladjust + invoiceadditionaladjust;
			}
			updateInvoice(invoicelist, indice);
			updateInvoiceDetails(invoicedetailslist, indice);
			if (rsAdditional != null) {
				rsAdditional.close();
			}
			if (pstmtAdditional != null) {
				pstmtAdditional.close();
			}
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
