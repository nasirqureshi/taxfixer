package fixtax2015;

public class TaxAdjustMatrix {
	public Integer invoicenumber;
	public String partnumber;
	public Float SoldPrice;
	public Float taxremovedamount;
	
	public Integer getInvoicenumber() {
		return invoicenumber;
	}
	public void setInvoicenumber(Integer invoicenumber) {
		this.invoicenumber = invoicenumber;
	}
	public String getPartnumber() {
		return partnumber;
	}
	public void setPartnumber(String partnumber) {
		this.partnumber = partnumber;
	}
	public Float getSoldPrice() {
		return SoldPrice;
	}
	public void setSoldPrice(Float soldPrice) {
		SoldPrice = soldPrice;
	}
	public Float getTaxremovedamount() {
		return taxremovedamount;
	}
	public void setTaxremovedamount(Float taxremovedamount) {
		this.taxremovedamount = taxremovedamount;
	}
}
