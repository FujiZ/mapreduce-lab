package triangle_count;

import java.io.IOException;
import java.util.Arrays;

public class TriangleCountDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        assert args.length == 4;
        AdjList.main(Arrays.copyOfRange(args, 0, 2));
        TriangleCount.main(Arrays.copyOfRange(args, 1, 4));
    }
}
