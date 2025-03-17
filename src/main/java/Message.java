
public class Message {

	private String timeStamp;
	private String seqkey;
	private String LoyaltyCard_ID;
	private String Price;
	private String Product;
	private String Supplier;
	private String Shop;
	

	
	public String getAsText() {
		return AsText;
	}
	public void setAsText(String asText) {
		AsText = asText;
	}
	private String AsText;

	public String getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}
	public String getSeqkey() {
		return seqkey;
	}
	public void setSeqkey(String seqkey) {
		this.seqkey = seqkey;
	}
	public String getLoyaltyCard_ID() {
		return LoyaltyCard_ID;
	}
	public void setLoyaltyCard_ID(String Loyal_ID) {
		LoyaltyCard_ID = Loyal_ID;
	}
	public String getPrice() {
		return Price;
	}
	public void setPrice(String price) {
		Price = price;
	}
	public String getProduct() {
		return Product;
	}
	public void setProduct(String product) {
		Product = product;
	}
	public String getSupplier() {
		return Supplier;
	}
	public void setSupplier(String supplier) {
		Supplier = supplier;
	}
	public String getShop() {
		return Shop;
	}
	public void setShop(String shop) {
		Shop = shop;
	}
	@Override
	public String toString() {
		return "Message [timeStamp=" + timeStamp + ", seqkey=" + seqkey + ", LoyaltyCard_ID=" + LoyaltyCard_ID
				+ ", Price=" + Price + ", Product=" + Product + ", Supplier=" + Supplier + ", Shop=" + Shop
				+ ", AsText=" + AsText + "]";
	}
	public Message(String timeStamp, String seqkey, String loyaltyCard_ID, String price, String product,
			String supplier, String shop, String asText) {
		this.timeStamp = timeStamp;
		this.seqkey = seqkey;
		LoyaltyCard_ID = loyaltyCard_ID;
		Price = price;
		Product = product;
		Supplier = supplier;
		Shop = shop;
		AsText = asText;
	}
	
	public Message() {
	}
	
	

	
		
}
