import org.apache.kafka.common.utils.Utils;

public class Murmur2Cli {
    public static void main (String[] args) throws Exception {
        for (String key : args) {
            System.out.println(String.format("%s\t0x%08x", key,
                                             Utils.murmur2(key.getBytes())));
        }
        /* If no args, print hash for empty string */
        if (args.length == 0)
            System.out.println(String.format("%s\t0x%08x", "",
                                             Utils.murmur2("".getBytes())));

    }
}
