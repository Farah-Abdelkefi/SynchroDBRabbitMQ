

// Define the message format as a class
public class ProductSale {
    private String date;
    private String region;
    private String product;
    private int qty;
    private float cost;
    private float amt;
    private float tax;
    private float total;


    // Add getters and setters for the fields
    public String getDate() {
        return date;
    }
    public void setDate(String date) {
        this.date = date;
    }
    public String getRegion() {
        return region;
    }
    public void setRegion(String region) {
        this.region = region;
    }
    public String getProduct() {
        return product;
    }
    public void setProduct(String product) {
        this.product = product;
    }
    public int getQty() {
        return qty;
    }
    public void setQty(int qty) {
        this.qty = qty;
    }
    public float getCost() {
        return cost;
    }
    public void setCost(float cost) {
        this.cost = cost;
    }
    public float getAmt() {
        return amt;
    }
    public void setAmt(float amt) {
        this.amt = amt;
    }
    public float getTax() {
        return tax;
    }
    public void setTax(float tax) {
        this.tax = tax;
    }
    public float getTotal() {
        return total;
    }
    public void setTotal(float total) {
        this.total = total;
    }
 
    
}
