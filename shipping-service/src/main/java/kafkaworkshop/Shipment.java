package kafkaworkshop;

public class Shipment {
    public enum Status {
        scheduled, shipped
    }

    public Shipment(String orderID, String shipmentID, Status status) {
        this.orderID = orderID;
        this.status = status;
    }

    private String orderID;
    private Status status;
    private String shipmentID;


    public String getShipmentID() {
        return shipmentID;
    }

    public void setShipmentID(String shipmentID) {
        this.shipmentID = shipmentID;
    }


    public String getOrderID() {
        return orderID;
    }

    public void setOrderID(String orderID) {
        this.orderID = orderID;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }
}