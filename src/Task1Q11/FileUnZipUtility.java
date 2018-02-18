package Task1Q11;

import java.io.BufferedOutputStream;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileUnZipUtility
{
 // Buffer size to read and write data 
	private static final int BUFFER_SIZE = 4096;
	private static Logger LOGGER = null; 
	
	public FileUnZipUtility()
	{
		LOGGER = Log.setLogHandler(FileUnZipUtility.class,"");
		LOGGER.setLevel(Level.INFO);
	}
	
   public void fileUnzip(String inputPath, String outputPath, String dataCleanedPath) throws IOException 
   {
	  
       File destinationPath = new File(outputPath);
     
       LOGGER.info("Input Path: "+inputPath);
	   //Creates a directory if the output path is not available
	   if(!destinationPath.exists()) 
	   {
		   destinationPath.mkdirs();
	   }
	  
	   //Read the input file using the input stram for zip files
	   File directory  = new File(inputPath);
	   File[] directoryList = directory.listFiles();
	   
	   if(directoryList!=null) {
	       for(File childDir : directoryList) {
	    	   LOGGER.info("File Name: : "+childDir);
	   ZipInputStream zipIn = new ZipInputStream(new FileInputStream(childDir));
	   ZipEntry zipEntry;
	   while((zipEntry = zipIn.getNextEntry())!= null)
	  
		{
		  //Extract only the csv file 
		  if(zipEntry.getName().endsWith(".csv"))
		  {
		  String zipEntryfilePath = outputPath + File.separator + zipEntry.getName();
		  
		  //String zipOutfilePath = dataCleanedPath + File.separator + zipEntry.getName().replace("csv", "txt");
		  String zipOutfilePath = dataCleanedPath + File.separator + "InputDataFile_Task1Q11.txt";
		  fileExtract(zipIn,zipEntryfilePath);	
		  LOGGER.info("File  : "+zipEntry.getName()+ " extracted");
		  cleanData(zipEntryfilePath,zipOutfilePath);
		  LOGGER.info("File  : "+zipEntry.getName()+ " processed");
		  deleteFile(zipEntryfilePath);
		  LOGGER.info("File  : "+zipEntry.getName()+ " deleted");
		  }
		}
		}	     
   } }
	
	private void fileExtract(ZipInputStream zipIn, String zipEntryfilePath) throws IOException
	{
	  BufferedOutputStream  outputStream = new BufferedOutputStream(new FileOutputStream(zipEntryfilePath));
	  
	   byte[] bytesIn = new byte[BUFFER_SIZE];
	   int read = 0;
	   
	   while((read = zipIn.read(bytesIn)) != -1) 
	   {
		   outputStream.write(bytesIn, 0, read);
	   }
	  
	   outputStream.close();
	 
    }																
	
	private void cleanData(String zipEntryfilePath, String zipOutfilePath) throws IOException 
	{
		BufferedReader brRead = new BufferedReader(new FileReader(zipEntryfilePath));
		String line;
		
		//Read every line in CSV, extract Origin and Destination Columns and write in new csv
		BufferedWriter brWrite = new BufferedWriter(new FileWriter(zipOutfilePath,true));
		//Skip header
		brRead.readLine();		
		int count = 0;
		while ((line = brRead.readLine()) != null) {
			
          String[] columns = line.split(",");
          //System.out.println(columns[6] + "," +columns[14] );
          
          brWrite.write(columns[6].replace("\"",""));
          brWrite.write(",");
          brWrite.write(columns[14].replace("\"",""));
          brWrite.write("\n");
          count = count+1;
		}
		
		LOGGER.info("Line Count for file: "+count);
		brRead.close();
		brWrite.close();
		
	}

	private void deleteFile(String zipOutfilePath)
	{
		File file = new File(zipOutfilePath);
		
	    if(file.exists()) 
	    {
	    	file.delete();
	    }   
	}
	
	public static void main(String[] args) throws IOException 
	{
		FileUnZipUtility fileUnZipUtility = new FileUnZipUtility();
		fileUnZipUtility.fileUnzip(
				"/Users/archana/Documents/Projects/Capstone/aviation/airline_origin_destination/",
				"/Users/archana/Documents/Projects/Capstone/input_data/task1/Q1.1/data_extracted",
				"/Users/archana/Documents/Projects/Capstone/input_data/task1/Q1.1/data_cleaned");
	
	}
}
