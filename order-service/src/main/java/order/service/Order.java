package order.service;

public class Order {
    public enum Status {
        PENDING, CANCELLED
    }

    public int id;
    public String product;
    public Status status;

    public Order(int id, String product, Status status) {
        this.id = id;
        this.product = product;
        this.status = status;
    }
}
