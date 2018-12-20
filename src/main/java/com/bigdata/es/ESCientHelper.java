//
//
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import org.elasticsearch.action.ActionFuture;
//import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
//import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
//import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
//import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
//import org.elasticsearch.client.Client;
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//
//public class ESCientHelper {
//	public static String host = "192.168.106.130";
//	public static Client client = null;
//	
//	public static Client getESClient(){
//		try {
//			client = TransportClient.builder().build()
//			     .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), 9300));
//		
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		}
//		return client;
//	}
//	
//	public static void closeESClient(){
//		if(client != null){
//			client.close();
//		}
//	}
//	
//	
//	public static void main(String[] args) {
//		Client client = ESCientHelper.getESClient();
//		System.out.println("连接成功!");
//		//createIndex("20180118");
//		deleteIndex("20180118");
//	}
//	
//	public static void createIndex(String indexName){
//		CreateIndexRequest request = new CreateIndexRequest(indexName);
//		 ActionFuture<CreateIndexResponse> response = getESClient().admin().indices().create(request);
//		 System.out.println(response.actionGet());
//	}
//	
//	public static void deleteIndex(String indexName){
//		DeleteIndexRequest request = new DeleteIndexRequest(indexName);
//		 ActionFuture<DeleteIndexResponse> response = getESClient().admin().indices().delete(request);
//		 System.out.println(response.actionGet());
//	}
//
//}
