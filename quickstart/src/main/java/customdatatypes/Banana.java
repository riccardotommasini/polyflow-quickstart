package customdatatypes;

public class Banana extends Fruit {
    public Banana(float weight, String status) {
        super(weight, status);
    }

    @Override
    public float getWeight() {
        return super.getWeight();
    }

    @Override
    public String getName() {
        return "Banana";
    }

    @Override
    public String getStatus() {
        return super.getStatus();
    }

}
