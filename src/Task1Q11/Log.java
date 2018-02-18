package Task1Q11;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Log {
	
    static Logger LOGGER = null; 	  
	public static Logger setLogHandler(@SuppressWarnings("rawtypes") Class className, String path) 
	{
		LOGGER = Logger.getLogger(className.getName());
		FileHandler fh;  
		
		try {  
		    // This block configure the logger with handler and formatter  
		    fh = new FileHandler(path+className.getName()+".log");  
		    LOGGER.addHandler(fh);
		    SimpleFormatter formatter = new SimpleFormatter();  
		    fh.setFormatter(formatter);  

		    // the following statement is used to log any messages  
		} catch (SecurityException e) {  
		    e.printStackTrace();  
		} catch (IOException e) {  
		    e.printStackTrace();  
		}
		return LOGGER;  
		
	}	
} 
