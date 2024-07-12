package customdatatypes;

public class Pear extends Fruit {


    public Pear(float weight, String status) {
        super(weight, status);
    }

    @Override
    public float getWeight() {
        return super.getWeight();
    }

    @Override
    public String getName() {
        return "Pear";
    }

    @Override
    public String getStatus() {
        return super.getStatus();
    }
}
