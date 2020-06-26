public class Query {

    private QueryType type;

    public Query(QueryType type) {
        this.type = type;
    }

    public String toString() {
        return this.type.toString();
    }
}