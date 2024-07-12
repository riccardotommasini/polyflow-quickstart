package customdatatypes;

public class Peach extends Fruit {
    public Peach(float weight, String status) {
        super(weight, status);
    }

    @Override
    public float getWeight() {
        return super.getWeight();
    }

    @Override
    public String getName() {
        return "Peach";
    }

    @Override
    public String getStatus() {
        return super.getStatus();
    }
}
