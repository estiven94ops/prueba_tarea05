package prueba_tarea3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.DirectoryIteratorException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;



public class Producer {
	
	static List<String> list = new ArrayList<String>();

    private static final String TASK_QUEUE_NAME = "task_queue";
    
	 //private static final String EXCHANGE_NAME = "logs";
    
   
    public static String encryptThisString(String input) 
    { 
        try { 
            // getInstance() method is called with algorithm SHA-512 
            MessageDigest md = MessageDigest.getInstance("SHA-512"); 
  
            // digest() method is called 
            // to calculate message digest of the input string 
            // returned as array of byte 
            byte[] messageDigest = md.digest(input.getBytes()); 
  
            // Convert byte array into signum representation 
            BigInteger no = new BigInteger(1, messageDigest); 
  
            // Convert message digest into hex value 
            String hashtext = no.toString(16); 
  
            // Add preceding 0s to make it 32 bit 
            while (hashtext.length() < 32) { 
                hashtext = "0" + hashtext; 
            } 
  
            // return the HashText 
            return hashtext; 
        } 
  
        // For specifying wrong message digest algorithms 
        catch (NoSuchAlgorithmException e) { 
            throw new RuntimeException(e); 
        } 
    } 
    
    public static void listarFicherosPorCarpeta(final File carpeta) {
        for (final File ficheroEntrada : carpeta.listFiles()) {
            if (ficheroEntrada.isDirectory()) {
                listarFicherosPorCarpeta(ficheroEntrada);
            } else {
                //System.out.println(ficheroEntrada.getName());
                //System.out.println(ficheroEntrada.getAbsolutePath());
            	
                //Almacena nombres de archivos a lista.
                list.add(ficheroEntrada.getAbsolutePath());
                
            	}
            }
        }

    public static void main(String[] argv) throws Exception {
    	

  		FileReader reader = new FileReader("C:\\Users\\Steven\\eclipse-workspace\\prueba_tarea3\\src\\properties//archive.properties");
  		
  		Properties p = new Properties();
  		
  		p.load(reader);
  		
        /*System.out.println(p.getProperty("user"));
        String myStr = p.getProperty("user");
        System.out.println(p.getProperty("password"));
        System.out.println(p.getProperty("host"));
        System.out.println(p.getProperty("virtualhost"));
        System.out.println(Integer.parseInt(p.getProperty("port")));
        System.out.println(p.getProperty("ruta_archivos"));*/
  	    
    	  
        ConnectionFactory factory = new ConnectionFactory();
        
        factory.setUsername(p.getProperty("user").trim());
        factory.setPassword(p.getProperty("password"));
        factory.setHost(p.getProperty("host"));
        factory.setVirtualHost(p.getProperty("virtualhost"));
        factory.setPort(Integer.parseInt(p.getProperty("port")));
        try (Connection connection = factory.newConnection();
        		
        		
            Channel channel = connection.createChannel()) {
            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

            //channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            //String message = String.join(" ", argv);
 
           // System.out.println ("Por favor introduzca ruta:");

           // String entradaTeclado = "";

            //@SuppressWarnings("resource")
			//Scanner entradaEscaner = new Scanner (System.in); //Creacion de un objeto Scanner

            //entradaTeclado = entradaEscaner.nextLine (); //Invocamos un metodo sobre un objeto Scanner
            
            final File carpeta = new File(p.getProperty("ruta_archivos"));
            		 listarFicherosPorCarpeta(carpeta);
            		 
            		 //String[] ficheros = list.toArray(new String[list.size()]);
            		 //System.out.println(list.get(0));

            	
          for(int i = 0; i < list.size(); i++) {
        	    		    	
    		        //System.out.println(file.getFileName());
        	  		List<String> list_palabras = new ArrayList<String>();
        	  		List<String> list_frecuencias = new ArrayList<String>();
    		        JSONObject myObject = new JSONObject();
    		    	
    		    	JSONArray ja = new JSONArray();
    		    	File archivo;
    		    	FileReader fr = null;
    		    	BufferedReader br = null;
    		    	
    		    	Map<String, Integer> palabras = new HashMap<String, Integer>();

    		    	
    		    	try {
    		    		archivo = new File(list.get(i));
    		    		fr = new FileReader(archivo);
    		    		br = new BufferedReader(fr);
    		    		String nombre_archivo = archivo.getName();
    		    	
    		    		//System.out.println("Lectura de archivo " + nombre_archivo);
    		    		
    		    		
    		    		
    		    		String linea;
    		    		
    		    		while ((linea = br.readLine()) != null) {
    		    			//System.out.println(linea);
    		    			
    		    			for (String palabra: linea.split(" ")) {
    		    				palabras.put(palabra, palabras.containsKey(palabra) ? palabras.get(palabra) + 1 : 1);
    		    			}
    		    			
    		      		}
    		    		
    			        /*System.out.println("HashCode Generated by SHA-512 for: "); 
    			        
    			        String s1 = "GeeksForGeeks"; 
    			        System.out.println(" " + s1 + " : " + encryptThisString(s1)); 
    			  
    			        String s2 = "hello world"; 
    			        System.out.println(" " + s2 + " : " + encryptThisString(s2)); 
    			        
    			        String s3 = archivo.getName(); 
    			        System.out.println(" " + s3 + " : " + encryptThisString(s3)); */
    		    	
    		    		
    		    		
    			        for (HashMap.Entry<String, Integer> entry : palabras.entrySet()) {
    			            //System.out.printf("Palabra '%s' con frecuencia %d\n", entry.getKey(), entry.getValue());
    			            //subdata.put(entry.getKey(), entry.getValue( ));
    			            JSONObject subdata = new JSONObject();
    			            
    			            subdata.put("palabra", entry.getKey());
    			            subdata.put("frecuencia", entry.getValue());
    			            
    			            ja.put(subdata);
    			            
    			        }
    			        /*Set<String> keys = palabras.keySet();
    			        for ( String key : keys ) {
    			            System.out.println( key );

    			        }*/
    			        
    			        /*for(Integer value: palabras.values()){
    			            System.out.println(value);
    			        }*/
    			        
    			        //System.out.println(palabras.get(0));
    			            
    			            //ja.put(""+entry.getKey()+"" + ":" + entry.getValue());
   
    			        
    			        
    			        
    			       
    			        myObject.put("id", encryptThisString(archivo.getName()));
    			        myObject.put("frecuencias", ja);
    			        //System.out.println(myObject);
    			        
    			        
    			        channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, myObject.toString().getBytes("UTF-8"));
    			        
    			        //channel.basicPublish(EXCHANGE_NAME, "", null, myObject.toString().getBytes("UTF-8"));
    		            System.out.println(" [x] Sent '" + myObject + "'");
    		            System.out.println("*****************************************************************************************************************************************");

    			        
    		    	}catch (IOException e) {
    		    		e.printStackTrace();
    		    	}
    		    }
    		} catch (IOException | DirectoryIteratorException ex) {
    		    System.err.println(ex);
    		}

            
        }
    

}