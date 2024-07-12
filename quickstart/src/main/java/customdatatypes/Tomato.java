package customdatatypes;

/*
 * Yes, Tomatoes are considered a Fruit
 */
public class Tomato extends Fruit {


    public Tomato(float weight, String status) {
        super(weight, status);
    }

    @Override
    public float getWeight() {
        return super.getWeight();
    }

    @Override
    public String getName() {
        return "Tomato";
    }

    @Override
    public String getStatus() {
        return super.getStatus();
    }
}
