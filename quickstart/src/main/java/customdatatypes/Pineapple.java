package customdatatypes;

public class Pineapple extends Fruit {
    public Pineapple(float weight, String status) {
        super(weight, status);
    }

    @Override
    public float getWeight() {
        return super.getWeight();
    }

    @Override
    public String getName() {
        return "Pineapple";
    }

    @Override
    public String getStatus() {
        return super.getStatus();
    }
}
