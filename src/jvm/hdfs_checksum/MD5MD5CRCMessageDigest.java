package hdfs_checksum;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.DataChecksum;

public class MD5MD5CRCMessageDigest extends MessageDigest {

    public static final String ALGORITHM_NAME = "MD5MD5CRC";

    private final int bytesPerCRC;
    private final int crcPerBlock;
    private int crcCount;
    private int bytesRead;

    private final byte[] crc = new byte[4];
    private DataOutputBuffer blockChecksumBuffer, md5DigestBuffer;

    private DataChecksum checksum;
    private MessageDigest md5Digest;

    private final int checksumType;

    public MD5MD5CRCMessageDigest(int bytesPerCRC, int crcPerBlock, int checksumType) throws NoSuchAlgorithmException {
        super(ALGORITHM_NAME);
        this.checksumType = checksumType;
        this.bytesPerCRC = bytesPerCRC;
        this.crcPerBlock = crcPerBlock;
        initialize();
    }

    @Override
    protected byte[] engineDigest() {
        try {
            if (bytesRead > 0)
                updateChecksumBuffer();

            if (blockChecksumBuffer.getLength() > 0)
                updateMD5DigestBuffer();

            md5Digest.update(md5DigestBuffer.getData());
            return md5Digest.digest();

        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void engineReset() {
        // TODO: check this
        blockChecksumBuffer.reset();
        md5DigestBuffer.reset();

        checksum.reset();
        md5Digest.reset();

        bytesRead = 0;
    }

    @Override
    protected void engineUpdate(byte input) {
        // checksum.update(input);
        bytesRead += 1;
        try {
            if (bytesRead == bytesPerCRC)
                updateChecksumBuffer();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void engineUpdate(byte[] input, int offset, int len) {
        int bytesRemaining = len;
        final int bytesToComplete = bytesPerCRC - bytesRead;
        int i = offset;
        try {
            if (bytesRemaining >= bytesToComplete) {
                checksum.update(input, offset, bytesToComplete);
                bytesRemaining -= bytesToComplete;
                i += bytesToComplete;
                updateChecksumBuffer();
            }

            while (bytesRemaining >= bytesPerCRC) {
                checksum.update(input, offset + i, bytesPerCRC);
                i += bytesPerCRC;
                bytesRemaining -= bytesPerCRC;
                updateChecksumBuffer();
            }

            if (bytesRemaining > 0)
                ;
            checksum.update(input, offset + i, bytesRemaining);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateChecksumBuffer() throws IOException {
        final int crcLen = checksum.writeValue(crc, 0, true);
        blockChecksumBuffer.write(crc, 0, crcLen);

        bytesRead = 0;
        crcCount += 1;
        if (crcCount == crcPerBlock)
            updateMD5DigestBuffer();
    }

    private void updateMD5DigestBuffer() throws IOException {
        md5Digest.update(blockChecksumBuffer.getData(), 0, blockChecksumBuffer.getLength());
        md5DigestBuffer.write(md5Digest.digest());
        blockChecksumBuffer.reset();
        md5Digest.reset();
        crcCount = 0;
    }

    private void initialize() throws NoSuchAlgorithmException {
        md5Digest = MessageDigest.getInstance("MD5");
        blockChecksumBuffer = new DataOutputBuffer();
        md5DigestBuffer = new DataOutputBuffer();
        checksum = DataChecksum.newDataChecksum(checksumType, bytesPerCRC);
    }
}
