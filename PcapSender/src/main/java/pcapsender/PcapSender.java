package pcapsender;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;

import java.util.concurrent.TimeUnit;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.*;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;
import java.nio.ByteBuffer;
import org.apache.activemq.ActiveMQConnection;

public class PcapSender {

    public static long lastData = 0;
	
    public static byte[] hexStringToByteArray(String s) 
    {
        int len = s.length();
        byte[] data = new byte[len];
       
	if((len % 2) != 0)
	{
            char a = s.charAt(len - 1);
            StringBuilder ns = new StringBuilder(s);
            ns.setCharAt(len - 1, '0');
            ns.append(a);
            s = ns.toString();
	}

	try {
            for (int i = 0; i < len; i += 2) 
            {
                data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
            }
	} catch(Exception e)
	{
            System.out.println("ERROR" + " " + len);
	}
	
        return data;
    }

    public static void writeByte(RandomAccessFile file, byte[] _byte) throws IOException {
        for(int i=0; i <= 3; i++)
        {
            file.write(_byte[i]);
        }
    }

    public static byte[] IntByte(int num) {
        byte[] _byte = new byte[4];
        for(int i = 0; i < 4; i++)
        {
            _byte[i] = (byte)(num >>> (i*8));
        }

        return _byte;
    }
    
    public static long bytesToLong(byte[] bytes) {
    	ByteBuffer buffer = ByteBuffer.allocate(8);
	buffer.put(bytes);
	buffer.flip();
	return buffer.getLong();
    }

    public static void GlobalHeader(RandomAccessFile Subor, byte[] MagicNumber, byte[] Version, byte[] Timestamp, byte[] Accuracy, byte[] SnapshotLength, byte[] Type) throws IOException {
        Subor.write(MagicNumber, 0, 4);
        Subor.write(Version, 0, 4);
        Subor.write(Timestamp, 0, 4);
        Subor.write(Accuracy, 0, 4);
        Subor.write(SnapshotLength, 0, 4);
        Subor.write(Type, 0, 4);
    }

    public static void PcapHeader(RandomAccessFile file, int _length) throws IOException {
        byte[] Timestamp = hexStringToByteArray("4fcdbac2");
        file.write(Timestamp, 0, 4);
        file.write(Timestamp, 0, 4);
        byte[] length = IntByte(_length);
        file.write(length, 0, 4);
        file.write(length, 0, 4); 
   }

    public static int ParsePackets(RandomAccessFile file) {
        int packetsWritten = 0;

        try
        {
            System.out.println("Parsing packets!");
            Configuration conf = HBaseConfiguration.create();
            conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));

            org.apache.hadoop.hbase.client.Connection connection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf("Agent"));

            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes("packet"));
            ResultScanner scanner = table.getScanner(scan);

            long newLastId = 0;
            for(Result result = scanner.next(); result != null; result = scanner.next())
            {
                byte[] row = result.getRow();
                long id = bytesToLong(row);
                newLastId = id;

                if(id > lastData)
                {
                    byte[] data = result.getValue(Bytes.toBytes("packet"), Bytes.toBytes("data"));

                    String s = Bytes.toString(data);

                    if(s != "")
                    {
                        s = s.replaceAll("\\s", "");
                        int length = (s.length()) / 2;

                        byte[] _byte = hexStringToByteArray(s);
                        PcapHeader(file, length);
                        file.write(_byte, 0, length);
                        packetsWritten++;	
                    }
                }
            }	

            System.out.println("Packets written: " + packetsWritten + " Last id: " + newLastId);
            lastData = newLastId;
            table.close();

        } catch (Exception e)
        {
            e.printStackTrace();
        }

        return packetsWritten;
    }
   
    public static void sendToQueue(String queue, File subor) throws FileNotFoundException, JMSException {
        String DEST_QUEUE = queue;
        javax.jms.Connection connection = null;

        try {
            System.out.println("Sending file to queue: " + queue);
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://147.175.98.24:61616?jms.blobTransferPolicy.defaultUploadUrl=http://147.175.98.24:8989/blobMessages/");

            connection = factory.createConnection();
            connection.start();

            ActiveMQSession session = (ActiveMQSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(DEST_QUEUE);
            MessageProducer producer = session.createProducer(destination);
            BlobMessage message = session.createBlobMessage(subor);

            message.setStringProperty("FILE.NAME", subor.getName());
            message.setLongProperty("FILE.SIZE", subor.length());
            message.setName("test1");
            producer.send(message);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            connection.close();
        }
    }
   
    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException, JMSException {
		
        while(true){
            DateFormat df = new SimpleDateFormat("MMddyyyyHHmmss");
            Date today = Calendar.getInstance().getTime();
            String reportDate = df.format(today);
            File subor = new File(reportDate + ".pcap");
            RandomAccessFile Subor = new RandomAccessFile(subor, "rw");

            byte[] MagicNumber = hexStringToByteArray("d4c3b2a1");
            byte[] Version = hexStringToByteArray("02000400");
            byte[] Timestamp = hexStringToByteArray("00000000");
            byte[] Accuracy = hexStringToByteArray("00000000");
            byte[] SnapshotLength = hexStringToByteArray("0000ffff");
            byte[] Type = hexStringToByteArray("01000000");

            GlobalHeader(Subor, MagicNumber, Version, Timestamp, Accuracy, SnapshotLength, Type);
            int packetsWritten = ParsePackets(Subor);
            Subor.close();

            if(packetsWritten > 0)
            {
                for(int i = 0; i < args.length; i++)
                    sendToQueue(args[i], subor); 
            }
            else
            {
                subor.delete();
            }

            TimeUnit.SECONDS.sleep(5);
        }
    }
}
