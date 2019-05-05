package sdkwrapper.runtimemgr;


import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import sdkwrapper.exceptions.BlockEventException;
import sdkwrapper.exceptions.ConfigurationException;
import sdkwrapper.exceptions.FabricRequestException;
import sdkwrapper.exceptions.FailedEndorsementException;
import sdkwrapper.exceptions.InfrastructureException;
import sdkwrapper.service.FabricServices;

public class End2EndWithEventing
{
  private RuntimeMgrIF   runtimeMgr     = null;
  private FabricServices fabricServices = null;
  
  public End2EndWithEventing( RuntimeMgrIF runtimeMgr )
  {
    this.runtimeMgr = runtimeMgr;
    
    fabricServices = runtimeMgr.getFabricServices();
  }
  
  public void submitTransaction( String[] payload ) 
  {
    try
    {
      fabricServices.requestTransaction( "green", "User1", payload, "invoke" );
    } catch( FabricRequestException | FailedEndorsementException | InfrastructureException e )
    {
      System.out.println( "Transaction error for payload " + payload + ". Error = " + e.getMessage() );
      e.printStackTrace();
    }
  }
  
  public String submitQuery( String[] payload ) 
  {
    try
    {
      String response = fabricServices.query( "green", "User1", payload, "query" );
      System.out.println( "Query response = " + response );
    } catch( FabricRequestException  e )
    {
      System.out.println( "Query error for payload " + payload + ". Error = " + e.getMessage() );
      e.printStackTrace();
    }
    
    return null;
  }
 
  public void waiting()
  {
    try 
    {
      TimeUnit.SECONDS.sleep(3);
    } 
    catch( InterruptedException e ) 
    {
      System.err.format("IOException: %s%n", e);
    } 
  }

  public static void main( String[] args )
  {
    final String configPath = "/usr/src/app/config/"; // Folder for Kubernetes configMap
    final String secretPath = "/usr/src/app/secret/"; // Folder for Kubernetes Secrets

    String contextPath   = null;
    String sdkConfigPath = null;

    if( args.length < 1 ) contextPath = configPath + "org-context.properties";
     else contextPath = args[0];
  
    if( args.length < 2 ) sdkConfigPath = secretPath + "config-client.json";
     else sdkConfigPath = args[1];
  
  
    RuntimeMgrIF runtimeMgr = new RuntimeMgr();
  
    try
    {
      runtimeMgr.initialize( contextPath, sdkConfigPath);
    } 
    catch( ConfigurationException | InfrastructureException e )
    {
      System.out.println( "Configuration Error. Stopping system. Error = " + e.getMessage() );
      System.exit( -1 );
    }
    
    // Now for the tests
    End2EndWithEventing test = new End2EndWithEventing( runtimeMgr );
    
    String[] tran1 = { "a", "b", "1" }; test.submitTransaction( tran1 ); //test.waiting();
    String[] tran2 = { "a", "b", "2" }; test.submitTransaction( tran2 ); //test.waiting();
    String[] tran3 = { "a", "b", "3" }; test.submitTransaction( tran3 ); //test.waiting();
    String[] tran4 = { "a", "b", "4" }; test.submitTransaction( tran4 ); //test.waiting();
    String[] tran5 = { "a", "b", "5" }; test.submitTransaction( tran5 ); test.waiting();
    String[] tran6 = { "a", "b", "6" }; test.submitTransaction( tran6 ); test.waiting();
    String[] tran7 = { "a", "b", "7" }; test.submitTransaction( tran7 ); test.waiting();
    String[] tran8 = { "a", "b", "8" }; test.submitTransaction( tran8 ); test.waiting();
    String[] tran9 = { "a", "b", "9" }; test.submitTransaction( tran9 ); test.waiting();
    String[] query1 = { "a" }; test.submitQuery( query1 );
 //   String[] query2 = { "a" }; test.submitQuery( query2 );
    
    Map<String, Long> blockHeights = null;
    try
    {
      blockHeights = runtimeMgr.getFabricServices().obtainBlockHeights();
    } catch( InfrastructureException e )
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    if( blockHeights != null )
    {
      Iterator<Entry<String, Long>> iter = blockHeights.entrySet().iterator();
      while( iter.hasNext() ) 
      {
        Map.Entry pair = (Map.Entry)iter.next();
        System.out.println( pair.getKey() + " = " + pair.getValue() );
      }
    }
  }

}
