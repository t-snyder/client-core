package sdkwrapper.runtimemgr;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import sdkwrapper.block.store.BlockEventSeqFileStore;
import sdkwrapper.block.store.BlockEventSeqStoreIF;
import sdkwrapper.config.ConfigProperties;
import sdkwrapper.error.ErrorCommandIF;
import sdkwrapper.error.ErrorController;
import sdkwrapper.events.BlockEventProcessorIF;
import sdkwrapper.events.BlockEventProcessorImpl;
import sdkwrapper.exceptions.BlockEventException;
import sdkwrapper.exceptions.ConfigurationException;
import sdkwrapper.exceptions.InfrastructureException;

import sdkwrapper.service.FabricServices;


/**
 * Provides the main method for starting up the sdk wrapper functionality.
 * 
 * @author tim
 *
 */
public class RuntimeMgr implements RuntimeMgrIF
{
  private static final Logger logger = LogManager.getLogger( RuntimeMgr.class );
  
  private FabricServices        fabricServices      = null;
  private ErrorController       errorController     = null;
  private BlockEventProcessorIF blockEventProcessor = null;  
  private BlockEventSeqStoreIF  blockSeqStore       = null;
  private ConfigProperties      config              = null;
  
  public ErrorController       getErrorController()     { return errorController;     }
  public FabricServices        getFabricServices()      { return fabricServices;      }
  public BlockEventProcessorIF getBlockEventProcessor() { return blockEventProcessor; }
  public BlockEventSeqStoreIF  getBlockSeqStore()       { return blockSeqStore;       }
  public ConfigProperties      getConfig()              { return config;              }
  
  
  @Override
  public void initialize(String appPropsPath, String sdkConfigPath) 
   throws ConfigurationException, InfrastructureException 
  {
    logger.info( "Initializing the RuntimeMgr" );
    
    errorController = new ErrorController( this );
	
    logger.info( "Obtaining the organization configuration properties file." );
    loadOrgContextConfig( appPropsPath  );
 
    // initialize the Block Sequence Store
    try
    {
      this.blockSeqStore = BlockEventSeqFileStore.getInstance();
    } catch( BlockEventException e )
    {
      throw new InfrastructureException( "Error initializing Block Sequence Store. Error = " + e.getMessage() );
    }

    
    // Initializing Fabric Services
    startFabricServices( sdkConfigPath );

    try
    {
      // Start Fabric Block Listeners
      this.blockEventProcessor = new BlockEventProcessorImpl( this, blockSeqStore );

      // Start the channel block event listeners
      fabricServices.initializeListeners();
    }
    catch( BlockEventException e )
    {
      throw new InfrastructureException( "Error starting Block listeners. Error = " + e.getMessage() );
    }
  }

  @Override
  public void runErrorCmd( ErrorCommandIF cmd ) 
  {
    cmd.processError();
  }

  
  private void loadOrgContextConfig( String propsPath )
   throws ConfigurationException
  {
	  config = new ConfigProperties( propsPath ); 
  }
  
  private void startFabricServices( String sdkConfigPath )
   throws InfrastructureException, ConfigurationException
  {
    fabricServices = new FabricServices( this );
        
    fabricServices.initialize( sdkConfigPath );
  }

  
  
  public static void main( String[] args )
  {
    final String configPath = "/usr/src/app/config/"; // Folder for Kubernetes configMap
    final String secretPath = "/usr/src/app/secret/"; // Folder for Kubernetes Secrets

    String  contextPath   = configPath + "org-context.properties";
    String  sdkConfigPath = secretPath + "config-client.json";
    
    if( args.length > 0 )
    {
      for( int i = 0; i < args.length; i++ )
      {
        if( "-c".compareTo( args[i]    ) == 0 ) { i++; contextPath = args[i]; continue; }
        if( "-s".compareTo( args[i]    ) == 0 ) { i++; sdkConfigPath = args[i]; continue; }
      }
    }	
	
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
  }
}
