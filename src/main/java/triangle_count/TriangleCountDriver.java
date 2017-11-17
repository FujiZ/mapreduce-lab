package triangle_count;

import java.io.IOException;

public class TriangleCountDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        assert args.length == 6;
        String[] adjListArgs = new String[3];
        adjListArgs[0] = args[0];   // input
        adjListArgs[1] = args[1];   // output
        adjListArgs[2] = args[3];   // directed or undirected
        AdjList.main(adjListArgs);
        String[] triangleCountArgs = new String[4];
        triangleCountArgs[0] = args[1]; // input
        triangleCountArgs[1] = args[2]; // output
        triangleCountArgs[2] = args[4]; // split size in MB
        triangleCountArgs[3] = args[5]; // reducer number
        TriangleCount.main(triangleCountArgs);
    }
}
