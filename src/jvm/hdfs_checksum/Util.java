package hdfs_checksum;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsProtoUtil;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.DataChecksum;

public class Util {
    private static final String CRCS_PER_BLOCK = "crcs-per-block";
    private static final String BLOCK_CHECKSUMS = "checksums";
    private static final String BLOCK_BOUNDARIES = "boundaries";
    private static final String MD5 = "md5";
    private static final String BLOCK_ID = "block-id";
    private static final String CHECKSUM_TYPE = "checksum-type";
    private static final String BYTES_PER_CRC = "bytes-per-crc";

    private static final Log LOG = LogFactory.getLog(Util.class);

    // code copied from DFSClient
    public static Map<Object, Object> blockChecksums(String src, Configuration conf) throws IOException {
        final ClientProtocol namenode = DFSUtil.createNamenode(conf);
        final SocketFactory socketFactory = NetUtils.getSocketFactory(conf, ClientProtocol.class);
        final int soTimeout = readSocketTimeout(conf);
        // get all block locations
        LocatedBlocks blockLocations = callGetBlockLocations(namenode, src, 0, Long.MAX_VALUE);
        if (null == blockLocations) {
            throw new FileNotFoundException("File does not exist: " + src);
        }
        List<LocatedBlock> locatedblocks = blockLocations.getLocatedBlocks();
        int bytesPerCRC = -1;
        DataChecksum.Type crcType = DataChecksum.Type.DEFAULT;
        long crcPerBlock = 0;
        boolean refetchBlocks = false;
        int lastRetriedIndex = -1;

        final Map<Object, Object> result = new HashMap<Object, Object>();
        final Collection<Object> checksums = new LinkedList<Object>();

        // get block checksum for each block
        for (int i = 0; i < locatedblocks.size(); i++) {
            if (refetchBlocks) { // re-fetch to get fresh tokens
                blockLocations = callGetBlockLocations(namenode, src, 0, Long.MAX_VALUE);
                if (null == blockLocations) {
                    throw new FileNotFoundException("File does not exist: " + src);
                }
                locatedblocks = blockLocations.getLocatedBlocks();
                refetchBlocks = false;
            }
            final LocatedBlock lb = locatedblocks.get(i);
            final ExtendedBlock block = lb.getBlock();
            final DatanodeInfo[] datanodes = lb.getLocations();

            // try each datanode location of the block
            final int timeout = 3000 * datanodes.length + soTimeout;
            boolean done = false;
            for (int j = 0; !done && j < datanodes.length; j++) {
                Socket sock = null;
                DataOutputStream out = null;
                DataInputStream in = null;

                try {
                    // connect to a datanode
                    sock = socketFactory.createSocket();
                    NetUtils.connect(sock, NetUtils.createSocketAddr(datanodes[j].getName()), timeout);
                    sock.setSoTimeout(timeout);
                    out =
                            new DataOutputStream(new BufferedOutputStream(NetUtils.getOutputStream(sock),
                                    HdfsConstants.SMALL_BUFFER_SIZE));
                    in = new DataInputStream(NetUtils.getInputStream(sock));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("write to " + datanodes[j].getName() + ": " + Op.BLOCK_CHECKSUM + ", block=" + block);
                    }
                    // get block MD5
                    new Sender(out).blockChecksum(block, lb.getBlockToken());
                    final BlockOpResponseProto reply = BlockOpResponseProto.parseFrom(HdfsProtoUtil.vintPrefixed(in));
                    if (reply.getStatus() != Status.SUCCESS) {
                        if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN && i > lastRetriedIndex) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Got access token error in response to OP_BLOCK_CHECKSUM " + "for file "
                                        + src + " for block " + block + " from datanode " + datanodes[j].getName()
                                        + ". Will retry the block once.");
                            }
                            lastRetriedIndex = i;
                            done = true; // actually it's not done; but we'll retry
                            i--; // repeat at i-th block
                            refetchBlocks = true;
                            break;
                        } else {
                            throw new IOException("Bad response " + reply + " for block " + block + " from datanode "
                                    + datanodes[j].getName());
                        }
                    }
                    final OpBlockChecksumResponseProto checksumData = reply.getChecksumResponse();
                    // read byte-per-checksum
                    final int bpc = checksumData.getBytesPerCrc();
                    if (i == 0) { // first block
                        bytesPerCRC = bpc;
                        result.put(BYTES_PER_CRC, bytesPerCRC);
                    } else if (bpc != bytesPerCRC) {
                        throw new IOException("Byte-per-checksum not matched: bpc=" + bpc + " but bytesPerCRC="
                                + bytesPerCRC);
                    }

                    // read crc-per-block
                    final long cpb = checksumData.getCrcPerBlock();
                    if (locatedblocks.size() > 1 && i == 0) {
                        crcPerBlock = cpb;
                        result.put(CRCS_PER_BLOCK, crcPerBlock);
                    }
                    // read md5
                    final String md5 = new String(Hex.encodeHex(checksumData.getMd5().toByteArray()));
                    // read crc-type
                    final DataChecksum.Type ct = HdfsProtoUtil.fromProto(checksumData.getCrcType());
                    if (i == 0) { // first block
                        crcType = ct;
                        result.put(CHECKSUM_TYPE, crcType.toString());
                    } else if (crcType != DataChecksum.Type.MIXED && crcType != ct) {
                        // if crc types are mixed in a file
                        // TODO: check this, is this valid?
                        crcType = DataChecksum.Type.MIXED;
                        result.put(CHECKSUM_TYPE, crcType);
                    }
                    done = true;
                    if (LOG.isDebugEnabled()) {
                        if (i == 0) {
                            LOG.debug("set bytesPerCRC=" + bytesPerCRC + ", crcPerBlock=" + crcPerBlock);
                        }
                        LOG.debug("got reply from " + datanodes[j].getName() + ": md5=" + md5);
                    }
                    checksums.add(blockChecksum(lb, md5));
                } catch (final IOException ie) {
                    LOG.warn("src=" + src + ", datanodes[" + j + "].getName()=" + datanodes[j].getName(), ie);
                } finally {
                    IOUtils.closeStream(in);
                    IOUtils.closeStream(out);
                    IOUtils.closeSocket(sock);
                }
            }
            if (!done) {
                throw new IOException("Fail to get block MD5 for " + block);
            }
        }
        result.put(BLOCK_CHECKSUMS, checksums);
        return result;
    }

    private static Object blockChecksum(LocatedBlock lb, String md5) {
        final Map<Object, Object> blkChecksum = new HashMap<Object, Object>();
        blkChecksum.put(BLOCK_ID, lb.getBlock().getBlockId());
        blkChecksum.put(MD5, md5);
        final List<Long> boundary = new ArrayList<Long>();
        boundary.add(lb.getStartOffset());
        boundary.add(lb.getBlockSize());
        blkChecksum.put(BLOCK_BOUNDARIES, boundary);
        return blkChecksum;
    }

    private static int readSocketTimeout(Configuration conf) {
        return conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, HdfsServerConstants.READ_TIMEOUT);
    }

    /**
     * @see ClientProtocol#getBlockLocations(String, long, long)
     */
    private static LocatedBlocks callGetBlockLocations(ClientProtocol namenode, String src, long start, long length)
            throws IOException {
        try {
            return namenode.getBlockLocations(src, start, length);
        } catch (final RemoteException re) {
            throw re.unwrapRemoteException(AccessControlException.class, FileNotFoundException.class,
                    UnresolvedPathException.class);
        }
    }
}
