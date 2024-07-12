package customdatatypes;

public class Apple extends Fruit {
    public Apple(float weight, String status) {
        super(weight, status);
    }

    @Override
    public float getWeight() {
        return super.getWeight();
    }

    @Override
    public String getName() {
        return "Apple";
    }

    @Override
    public String getStatus() {
        return super.getStatus();
    }
}
