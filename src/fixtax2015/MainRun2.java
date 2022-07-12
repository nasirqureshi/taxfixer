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

public class MainRun2 {


	public static String targedb = "local";

	public static void main(String[] args) {

		StringBuffer sb = new StringBuffer();
		// adjust variables
		int i = 0;
		Float targetamountadjust = 0.00f;
		Date adjustdatefrom = null;
		Date adjustdateto = null;
		Map<Integer, Integer> invoiceList = new LinkedHashMap<Integer, Integer>();
		List<String> customerList = new ArrayList<String>();
		String sqlAdjust = "SELECT * from taxadjust";

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

	private static Map<Integer, Integer> getAllMatchedAdditionalInvoices(Date adjustdatefrom, Date adjustdateto) {
		Map<Integer, Integer> additionalinvoiceList = new LinkedHashMap<Integer, Integer>();
		String sqlMatchInvoices = "SELECT i.invoicenumber , COUNT(id.InvoiceNumber) cnt FROM invoice i, invoicedetails id "
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
			// TODO Auto-generated catch block
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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

	private static void processEntries(Map<Integer, Integer> invoiceList, List<String> customerList,
			Float targetamount) {
		Float finalamountadjust = 0.00f;
		Connection connection = null;
		PreparedStatement pstmtTaxer = null;
		ResultSet rsTaxer = null;
		String sqlTaxer = "SELECT i.invoicenumber , id.partnumber, id.SoldPrice, ROUND( (( id.SoldPrice * ( (i.tax * 100 ) / i.invoicetotal ) ) / 100 ) , 2 )AS taxremovedamount "
				+ " FROM invoice i , invoicedetails id WHERE i.InvoiceNumber = id.InvoiceNumber AND i.invoicenumber = ? ORDER BY ID.SoldPrice DESC LIMIT ?;";

		try {

			BvasConFatory bvasConFactory = new BvasConFatory(targedb);
			connection = bvasConFactory.getConnection();
			List<TaxAdjustMatrix> adjustmatrixlist = new ArrayList<TaxAdjustMatrix>();
			for (Map.Entry<Integer, Integer> entry : invoiceList.entrySet()) {
				if (finalamountadjust < targetamount) {
					pstmtTaxer = connection.prepareStatement(sqlTaxer);
					pstmtTaxer.setInt(1, entry.getKey());
					pstmtTaxer.setInt(2, entry.getValue());
					rsTaxer = pstmtTaxer.executeQuery();
					while (rsTaxer.next()) {
						TaxAdjustMatrix taxadjustmatrix = new TaxAdjustMatrix();
						finalamountadjust = finalamountadjust + rsTaxer.getFloat("taxremovedamount");
						taxadjustmatrix.setInvoicenumber(entry.getKey());
						taxadjustmatrix.setPartnumber(rsTaxer.getString("partnumber"));
						taxadjustmatrix.setTaxremovedamount(rsTaxer.getFloat("taxremovedamount"));
						taxadjustmatrix.setSoldPrice(rsTaxer.getFloat("soldprice"));
						adjustmatrixlist.add(taxadjustmatrix);
						if (finalamountadjust > targetamount) {
							System.out.println("*" + finalamountadjust);
							System.out.println("**" + adjustmatrixlist.size());
							System.out.println("***" + customerList.size());

							updateFinalTaxNumbers(adjustmatrixlist, customerList);

							break;
						}
						// System.out.println(
						// rsTaxer.getString("invoicenumber") + "#" +
						// rsTaxer.getString("partnumber") + "#" +
						// rsTaxer.getFloat("taxremovedamount") + "#" +
						// rsTaxer.getFloat("soldprice") + "#" + ( (
						// rsTaxer.getFloat("taxremovedamount") * 100)/
						// rsTaxer.getFloat("soldprice"))) ;
					}
					rsTaxer.close();
					pstmtTaxer.close();
				} // if loop

			} // for loop

			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void updateFinalTaxNumbers(List<TaxAdjustMatrix> adjustmatrixlist, List<String> customerList) {
		String updateInvoiceSQL = "UPDATE invoice SET   Tax = Tax - ?, CustomerID = ? WHERE invoicenumber = ?";
		String updateInvoiceDetailsSQL = "UPDATE invoicedetails SET soldprice = soldprice + ?  WHERE invoicenumber = ? AND PartNumber = ?";
		Connection connection = null;
		PreparedStatement pstmtUpdateInvoice = null;
		PreparedStatement pstmtdeleteInvoiceDetails = null;
		BvasConFatory bvasConFactory = new BvasConFatory(targedb);
		try {
			Random rand = new Random();

			connection = bvasConFactory.getConnection();
			for (TaxAdjustMatrix taxmatrix : adjustmatrixlist) {
				int num = rand.nextInt(customerList.size() - 1);
				// System.out.println(customerList.get(num));
				pstmtUpdateInvoice = connection.prepareStatement(updateInvoiceSQL);
				
				pstmtUpdateInvoice.setFloat(1, taxmatrix.getTaxremovedamount());
				pstmtUpdateInvoice.setString(2, customerList.get(num));
				pstmtUpdateInvoice.setInt(3, taxmatrix.getInvoicenumber());
				pstmtUpdateInvoice.executeUpdate();
				pstmtdeleteInvoiceDetails = connection.prepareStatement(updateInvoiceDetailsSQL);
				pstmtdeleteInvoiceDetails.setFloat(1, taxmatrix.getTaxremovedamount());
				pstmtdeleteInvoiceDetails.setInt(2, taxmatrix.getInvoicenumber());
				pstmtdeleteInvoiceDetails.setString(3, taxmatrix.getPartnumber());
				pstmtdeleteInvoiceDetails.executeUpdate();
			}
			pstmtUpdateInvoice.close();
			pstmtdeleteInvoiceDetails.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

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

	private static Map<Integer, Integer> getAllMatchedInvoices(Date adjustdatefrom, Date adjustdateto) {
		Map<Integer, Integer> invoiceList = new LinkedHashMap<Integer, Integer>();
		String sqlMatchInvoices = "SELECT i.invoicenumber , COUNT(id.InvoiceNumber) cnt FROM invoice i, invoicedetails id "
				+ " WHERE i.InvoiceNumber = id.InvoiceNumber " + " AND i.orderdate BETWEEN  ? AND ? "
				+ " AND ( (i.CustomerID IN (SELECT customerid FROM customer WHERE TaxID = 'N')) OR (customerid='1111111111') OR ('2222222222') )  "
				+ " and i.tax > 0"
				+ " and i.returnedinvoice= 0 GROUP BY i.invoicenumber  having cnt >= 2 ORDER BY  customerid ";

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
				invoiceList.put(rsAdjust.getInt("invoicenumber"), rsAdjust.getInt("cnt"));
				System.out.println(rsAdjust.getInt("invoicenumber"));
			}

			rsAdjust.close();
			pstmtAdjust.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return invoiceList;
	}


}
