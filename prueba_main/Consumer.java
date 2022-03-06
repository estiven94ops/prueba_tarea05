package prueba_tarea3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;
import java.util.stream.Stream;

import org.json.JSONObject;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Consumer {
	

	
    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws Exception {
    	
	    
	    
    	FileReader reader = new FileReader("C:\\Users\\Steven\\eclipse-workspace\\prueba_tarea3\\src\\properties//archive.properties");
  		
  		Properties c = new Properties();
  		
  		c.load(reader);
  		
  	
    	
    	
  		
        /*System.out.println(c.getProperty("user"));
        String myStr = c.getProperty("user");
        System.out.println(c.getProperty("password"));
        System.out.println(c.getProperty("host"));
        System.out.println(c.getProperty("virtualhost"));
        System.out.println(Integer.parseInt(c.getProperty("port")));
        System.out.println(c.getProperty("ruta_archivos"));*/
  	    
    	  
        ConnectionFactory factory = new ConnectionFactory();
        
        factory.setUsername(c.getProperty("user").trim());
        factory.setPassword(c.getProperty("password"));
        factory.setHost(c.getProperty("host"));
        factory.setVirtualHost(c.getProperty("virtualhost"));
        factory.setPort(Integer.parseInt(c.getProperty("port")));

        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        boolean procesado = false;

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        channel.basicQos(1);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        	
            String message = new String(delivery.getBody(), "UTF-8");
            
            JSONObject json = new JSONObject(message);
            String id = json.getString("id");

            	        

            System.out.println(" [x] Received '" + message + "'");
            
            Scanner entrada = null;
            String linea;
            int numeroDeLinea = 1;
            boolean contiene = false;
            Scanner sc = new Scanner(System.in);

            //Para seleccionar el archivo
            //JFileChooser j = new JFileChooser();
            //j.showOpenDialog(j);

            //Introducimos el texto a buscar
            //System.out.print("Introduce texto a buscar: ");
            String texto = id;

            try {
                //guardamos el path del fichero en la variable ruta
                //String ruta = j.getSelectedFile().getAbsolutePath();
                //creamos un objeto File asociado al fichero seleccionado
                File f = new File(c.getProperty("frecuencia_IDs"));
                //creamos un Scanner para leer el fichero
                entrada = new Scanner(f);
                //mostramos el nombre del fichero
                //System.out.println("Archivo: " + f.getName());
                //mostramos el texto a buscar
                //System.out.println("Texto a buscar: " + texto);
                while (entrada.hasNext()) { //mientras no se llegue al final del fichero
                    linea = entrada.nextLine();  //se lee una linea
                    if (linea.contains(texto)) {   //si la linea contiene el texto buscado se muestra por pantalla         
                        //System.out.println("Linea " + numeroDeLinea + ": " + linea);
                        contiene = true;
                    }
                    numeroDeLinea++; //se incrementa el contador de lineas
                }
                if(!contiene){ //si el archivo no contienen el texto se muestra un mensaje indicandolo

                    //System.out.println(texto + " no se ha encontrado en el archivo");
                }
            } catch (FileNotFoundException e) {
                System.out.println(e.toString());
            } catch (NullPointerException e) {
                System.out.println(e.toString() + "No ha seleccionado ningun archivo");
            } catch (Exception e) {
                System.out.println(e.toString());
            } finally {
                if (entrada != null) {
                    entrada.close();
                }
            }
            
            
            //****************************LECTURA DE FICHERO CON IDs***********************//
                	if (contiene == true) {
                        try {
                            doWork(message);
                        } finally {
                        	System.out.println("*****EL ARCHIVO YA FUE PROCESADO*******");
                            System.out.println(" [x] Done ");
                            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                            
                            HashMap<String, Integer> hashMap = new HashMap<>();
                        	
                        	
                        	try {

                        	   	   
                                File f = new File(c.getProperty("frecuencia_persistencia"));

                                FileInputStream fis = new FileInputStream(f);

                                Properties p = new Properties();

                                p.load(fis);

                                fis.close();



                                Enumeration en = p.keys();



                                //System.out.println("Las variables y sus valores son");

                                while (en.hasMoreElements()) {

                                    String key = (String) en.nextElement();

                                    int value = Integer.parseInt(p.getProperty(key));
                                    
                                    hashMap.put(key, value);
                                    
                                    //System.out.println(key + ": " + value);

                                }

                                //System.out.println(hashMap);
                                System.out.println("\nLista de 10 palabras mas frecuentes");
                                hashMap.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed()).limit(10).forEach(System.out::println);
                        		//sorted.forEach(System.out::println);
                                /*List<Entry<Integer, Integer>> list = new ArrayList<>(hashMap.entrySet());
                        			list.sort(Entry.comparingByValue());
                        			list.forEach(System.out::println);*/
                                
                                


                            } catch (Exception e) {

                                System.out.println("Exception: " + e.getMessage());

                            }
                        } 
                	} else { 
                		try {
                        doWork(message);
                    } finally {
                    	//**************************************************GUARDADO DE ID en archivo para persistencia*****************************************************//
                    	try {
                            String ruta = c.getProperty("frecuencia_IDs");
                            //String ruta_2 = "C:\\Users\\Steven\\Desktop\\frecuencias.txt";
                            String contenido =System.lineSeparator()+ id;
                           //String contenido2 =System.lineSeparator()+ palabra + " " +  frecuencia;
                            File file = new File(ruta);
                            //File file2 = new File(ruta_2);
                            // Si el archivo no existe es creado
                            if (!file.exists()) {
                                file.createNewFile();
                                //file2.createNewFile();
                            }
                            FileWriter fw = new FileWriter(file, true);
                            BufferedWriter bw = new BufferedWriter(fw);
                            bw.write(contenido);
                            bw.close();
                            
                            //FileWriter fw1 = new FileWriter(file2, true);
                            //BufferedWriter bw1 = new BufferedWriter(fw1);
                            //bw1.write(contenido2);
                            //bw1.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    	
                    	int ciclo = json.getJSONArray("frecuencias").length();
                    	
                    	for (int i = 0; i < ciclo; i++) {
                        	String palabra = json.getJSONArray("frecuencias").getJSONObject(i).getString("palabra");
                        	int frecuencia = json.getJSONArray("frecuencias").getJSONObject(i).getInt("frecuencia");
                        	//System.out.println(palabra);
                        	
                        	
                        	//String nombreFichero = ("C:\\Users\\Steven\\Desktop\\frecuencias.txt");
                        	
                            try {
                                    //String ruta = "C:\\Users\\Steven\\Desktop\\prueba.txt";
                                    String ruta_2 = c.getProperty("frecuencia_persistencia");
                                    //String contenido =System.lineSeparator()+ id;
                                    String contenido2 =System.lineSeparator()+ palabra + " " +  frecuencia;
                                    //File file = new File(ruta);
                                    File file2 = new File(ruta_2);
                                    // Si el archivo no existe es creado
                                    if (!file2.exists()) {
                                        //file.createNewFile();
                                        file2.createNewFile();
                                    } else {
                                    
                                    FileWriter fw1 = new FileWriter(file2, true);
                                    BufferedWriter bw1 = new BufferedWriter(fw1);
                                    bw1.write(contenido2);
                                    bw1.close();
                                    }
                                    
                                    //FileWriter fw = new FileWriter(file, true);
                                    //BufferedWriter bw = new BufferedWriter(fw);
                                    //bw.write(contenido);
                                    //bw.close();

                                } catch (Exception e) {
                                    e.printStackTrace();
                                }

                            }
                    	
                    	
                    	HashMap<String, Integer> hashMap = new HashMap<>();
                    	
                    	
                    	try {

                    	   	   
                            File f = new File(c.getProperty("frecuencia_persistencia"));

                            FileInputStream fis = new FileInputStream(f);

                            Properties p = new Properties();

                            p.load(fis);

                            fis.close();



                            Enumeration en = p.keys();



                            //System.out.println("Las variables y sus valores son");

                            while (en.hasMoreElements()) {

                                String key = (String) en.nextElement();

                                int value = Integer.parseInt(p.getProperty(key));
                                
                                hashMap.put(key, value);
                                
                                //System.out.println(key + ": " + value);

                            }

                            //System.out.println(hashMap);
                            System.out.println("\nLista de 10 palabras mas frecuentes");
                            hashMap.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed()).limit(10).forEach(System.out::println);
                    		//sorted.forEach(System.out::println);
                            /*List<Entry<Integer, Integer>> list = new ArrayList<>(hashMap.entrySet());
                    			list.sort(Entry.comparingByValue());
                    			list.forEach(System.out::println);*/


                        } catch (Exception e) {

                            System.out.println("Exception: " + e.getMessage());

                        }

                        System.out.println(" [x] Done");
                        
                        
                        
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        
                    	}
                	} 
    	            
                	
        };
        channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> { });

        
    }

    private static void doWork(String task) {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    
    
    public static  void connection(String message) {
	    ServerSocket server = null;
	    Socket client = null;
	    //InputStream is;
	    InputStream is;
	    //int n;

	    DataInputStream in;
	    DataOutputStream out;
	    
	   
	    try {
	        server = new ServerSocket(5000);
	        System.out.println("servidor iniciado");
	        
	        FileReader reader = new FileReader("C:\\Users\\Steven\\eclipse-workspace\\prueba_tarea3\\src\\properties//archive.properties");
      		
      		Properties c = new Properties();
      		
      		c.load(reader);
	        
      		while(true) {
	        	client = server.accept();
	        	
	        	System.out.println("cliente conectado");
	        	
	        	in = new DataInputStream(client.getInputStream());
	        	out = new DataOutputStream(client.getOutputStream());
	        	
	        	//mensaje que llega desde el otro consumidor
	        	String mensaje_llega = in.readUTF();
	        	
	        	System.out.println("MENSAJE QUE LLEGO DEL CONSUMIDOR_02" + mensaje_llega);
	        	
	        	//mensaje que mandamos al otro consumidor
	        	out.writeUTF(message);
	        	
	        	
	        	JSONObject json = new JSONObject(mensaje_llega);
	            String id = json.getString("id");
	            	        

	            System.out.println(" MENSAJE QUE ENVIAMOS AL CONSUMIDOR_02 '" + message + "'");
	            
	            Scanner entrada = null;
	            String linea;
	            int numeroDeLinea = 1;
	            boolean contiene = false;
	            Scanner sc = new Scanner(System.in);

	            //Para seleccionar el archivo
	            //JFileChooser j = new JFileChooser();
	            //j.showOpenDialog(j);

	            //Introducimos el texto a buscar
	            //System.out.print("Introduce texto a buscar: ");
	            String texto = id;

	            try {
	                //guardamos el path del fichero en la variable ruta
	                //String ruta = j.getSelectedFile().getAbsolutePath();
	                //creamos un objeto File asociado al fichero seleccionado
	                File f = new File(c.getProperty("frecuencia_IDs"));
	                //creamos un Scanner para leer el fichero
	                entrada = new Scanner(f);
	                //mostramos el nombre del fichero
	                //System.out.println("Archivo: " + f.getName());
	                //mostramos el texto a buscar
	                //System.out.println("Texto a buscar: " + texto);
	                while (entrada.hasNext()) { //mientras no se llegue al final del fichero
	                    linea = entrada.nextLine();  //se lee una linea
	                    if (linea.contains(texto)) {   //si la linea contiene el texto buscado se muestra por pantalla         
	                        //System.out.println("Linea " + numeroDeLinea + ": " + linea);
	                        contiene = true;
	                    }
	                    numeroDeLinea++; //se incrementa el contador de lineas
	                }
	                if(!contiene){ //si el archivo no contienen el texto se muestra un mensaje indicandolo

	                    //System.out.println(texto + " no se ha encontrado en el archivo");
	                }
	            } catch (FileNotFoundException e) {
	                System.out.println(e.toString());
	            } catch (NullPointerException e) {
	                System.out.println(e.toString() + "No ha seleccionado ningun archivo");
	            } catch (Exception e) {
	                System.out.println(e.toString());
	            } finally {
	                if (entrada != null) {
	                    entrada.close();
	                }
	            }
	            
	            
	            //****************************LECTURA DE FICHERO CON IDs***********************//
	                	if (contiene == true) {

	                        	System.out.println("*****EL ARCHIVO YA FUE PROCESADO*******");

	                        } else { 
	                    	//**************************************************GUARDADO DE ID en archivo para persistencia*****************************************************//
	                    	try {
	                            String ruta = c.getProperty("frecuencia_IDs");
	                            //String ruta_2 = "C:\\Users\\Steven\\Desktop\\frecuencias.txt";
	                            String contenido =System.lineSeparator()+ id;
	                           //String contenido2 =System.lineSeparator()+ palabra + " " +  frecuencia;
	                            File file = new File(ruta);
	                            //File file2 = new File(ruta_2);
	                            // Si el archivo no existe es creado
	                            if (!file.exists()) {
	                                file.createNewFile();
	                                //file2.createNewFile();
	                            }
	                            FileWriter fw = new FileWriter(file, true);
	                            BufferedWriter bw = new BufferedWriter(fw);
	                            bw.write(contenido);
	                            bw.close();
	                            
	                            //FileWriter fw1 = new FileWriter(file2, true);
	                            //BufferedWriter bw1 = new BufferedWriter(fw1);
	                            //bw1.write(contenido2);
	                            //bw1.close();
	                        } catch (Exception e) {
	                            e.printStackTrace();
	                        }
	                    	
	                    	int ciclo = json.getJSONArray("frecuencias").length();
	                    	
	                    	for (int i = 0; i < ciclo; i++) {
	                        	String palabra = json.getJSONArray("frecuencias").getJSONObject(i).getString("palabra");
	                        	int frecuencia = json.getJSONArray("frecuencias").getJSONObject(i).getInt("frecuencia");
	                        	//System.out.println(palabra);
	                        	
	                        	
	                        	//String nombreFichero = ("C:\\Users\\Steven\\Desktop\\frecuencias.txt");
	                        	
	                            try {
	                                    //String ruta = "C:\\Users\\Steven\\Desktop\\prueba.txt";
	                                    String ruta_2 = c.getProperty("frecuencia_persistencia");
	                                    //String contenido =System.lineSeparator()+ id;
	                                    String contenido2 =System.lineSeparator()+ palabra + " " +  frecuencia;
	                                    //File file = new File(ruta);
	                                    File file2 = new File(ruta_2);
	                                    // Si el archivo no existe es creado
	                                    if (!file2.exists()) {
	                                        //file.createNewFile();
	                                        file2.createNewFile();
	                                    } else {
	                                    
	                                    FileWriter fw1 = new FileWriter(file2, true);
	                                    BufferedWriter bw1 = new BufferedWriter(fw1);
	                                    bw1.write(contenido2);
	                                    bw1.close();
	                                    }
	                                    
	                                    //FileWriter fw = new FileWriter(file, true);
	                                    //BufferedWriter bw = new BufferedWriter(fw);
	                                    //bw.write(contenido);
	                                    //bw.close();

	                                } catch (Exception e) {
	                                    e.printStackTrace();
	                                }

	                            }
	                    	
	                    	
	                    	HashMap<String, Integer> hashMap = new HashMap<>();
	                    	
	                    	
	                    	try {

	                    	   	   
	                            File f = new File(c.getProperty("frecuencia_persistencia"));

	                            FileInputStream fis = new FileInputStream(f);

	                            Properties p = new Properties();

	                            p.load(fis);

	                            fis.close();



	                            Enumeration en = p.keys();



	                            //System.out.println("Las variables y sus valores son");

	                            while (en.hasMoreElements()) {

	                                String key = (String) en.nextElement();

	                                int value = Integer.parseInt(p.getProperty(key));
	                                
	                                hashMap.put(key, value);
	                                
	                                //System.out.println(key + ": " + value);

	                            }

	                            //System.out.println(hashMap);
	                            System.out.println("\nLista de 10 palabras mas frecuentes");
	                            hashMap.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed()).limit(10).forEach(System.out::println);
	                    		//sorted.forEach(System.out::println);
	                            /*List<Entry<Integer, Integer>> list = new ArrayList<>(hashMap.entrySet());
	                    			list.sort(Entry.comparingByValue());
	                    			list.forEach(System.out::println);*/


	                        } catch (Exception e) {

	                            System.out.println("Exception: " + e.getMessage());

	                        }
	                       }


	        	
	    	    client.close();
	    	    System.out.println("cliente desconectado");         	
      		}
	
	     } catch (Exception e){
	            System.err.println("Error de conexión con el cliente");
	            //System.out.println("MENSAJE DEL SERVIDOR" + message);
	            
	            
	       }

    }
}
