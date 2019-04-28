package sdkwrapper.runtimemgr;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;

import org.hyperledger.fabric.sdk.exception.CryptoException;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric_ca.sdk.exception.EnrollmentException;

import sdkwrapper.exceptions.ConfigurationException;
import sdkwrapper.exceptions.InfrastructureException;
import sdkwrapper.service.FabricServices;

public class TestSecrets 
{
  public void listFilesForFolder( final File folder) {
    for (final File fileEntry : folder.listFiles()) {
        if (fileEntry.isDirectory()) {
            listFilesForFolder(fileEntry);
        } else {
            System.out.println(fileEntry.getName());
        }
    }
}

	public static void main(String[] args) 
	 throws ConfigurationException 
	{
		final String configPath = "/usr/src/app/config/";
		final String secretPath = "/usr/src/app/secret/";

    final File configFolder = new File( configPath );
    if( configFolder != null && configFolder.isDirectory() )
    {
System.out.println( "Found config directory" );

      for( File fileEntry : configFolder.listFiles() )
      {
        if( fileEntry.isFile() && fileEntry.getName().compareTo( "org-context.properties" ) == 0 )
        {
System.out.println( "Found config file" );
          FileReader fr = null;
          try
          {
            fr = new FileReader( fileEntry );
            
            int i; 
            while ((i=fr.read()) != -1) 
              System.out.print((char) i); 
          } catch( FileNotFoundException e )
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } catch( IOException e )
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          
        }
      }
    }
    
    final File secretFolder = new File( secretPath );
    if( secretFolder != null && secretFolder.isDirectory() )
    {
System.out.println(  "Found secret directory" );

      for( File fileEntry : secretFolder.listFiles() )
      {
        if( fileEntry.isFile() && fileEntry.getName().compareTo( "config-client.json" ) == 0 )
        {
System.out.println( "Found secret file" );

          FileReader fr = null;
          try
          {
            fr = new FileReader( fileEntry );
            
            int i; 
            while ((i=fr.read()) != -1) 
              System.out.print((char) i); 
            
            // Test JSON Config file
            
          } catch( FileNotFoundException e )
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } catch( IOException e )
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

          FabricServices fabricServices = new FabricServices( null );
          try
          {
            fabricServices.initialize( fileEntry.getAbsolutePath() );
          } 
          catch( ConfigurationException e )
          {
            throw new ConfigurationException( "Error Starting FabricServices. Error = " + e.getMessage() );
          }
          catch( InfrastructureException e )
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

          
        }
      }
    }
System.out.println(  "All Done" );    
	
	

	}

}
