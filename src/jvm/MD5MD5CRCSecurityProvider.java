import java.security.MessageDigest;
import java.security.Provider;
import java.security.Security;

public class MD5MD5CRCSecurityProvider extends Provider {
    private static final long serialVersionUID = -6874909498855232422L;
    private static boolean registered = false;

    protected MD5MD5CRCSecurityProvider() {
        super("Hadoop-MD5MD5CRC", 1.0, "Hadoop MD5MD5CRC Security Provider v1.0");
        put(MessageDigest.class.getSimpleName() + "." + MD5MD5CRCMessageDigest.ALGORITHM_NAME,
                MD5MD5CRCMessageDigest.class.getName());
    }

    public static synchronized void registerProvider() {
        if (!registered) {
            Security.addProvider(new MD5MD5CRCSecurityProvider());
            registered = true;
        }
    }
}
