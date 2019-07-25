package sdkwrapper.runtimemgr;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.hyperledger.fabric.sdk.helper.Config;

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
 * Provides the main method for starting up the client app with sdk wrapper functionality.
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

//    try
//    {
//      // Start Fabric Block Listeners
//      this.blockEventProcessor = new BlockEventProcessorImpl( this, blockSeqStore );
//
//      // Start the channel block event listeners
//      fabricServices.initializeListeners();
//    }
//    catch( BlockEventException e )
//    {
//      throw new InfrastructureException( "Error starting Block listeners. Error = " + e.getMessage() );
//    }
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

	  if( config.hasProperty( Config.PROPOSAL_WAIT_TIME                       )) System.setProperty( Config.PROPOSAL_WAIT_TIME,                       config.getProperty( Config.PROPOSAL_WAIT_TIME ));
    if( config.hasProperty( Config.CHANNEL_CONFIG_WAIT_TIME                 )) System.setProperty( Config.CHANNEL_CONFIG_WAIT_TIME,                 config.getProperty( Config.CHANNEL_CONFIG_WAIT_TIME ));
    if( config.hasProperty( Config.TRANSACTION_CLEANUP_UP_TIMEOUT_WAIT_TIME )) System.setProperty( Config.TRANSACTION_CLEANUP_UP_TIMEOUT_WAIT_TIME, config.getProperty( Config.TRANSACTION_CLEANUP_UP_TIMEOUT_WAIT_TIME ));
    if( config.hasProperty( Config.ORDERER_RETRY_WAIT_TIME                  )) System.setProperty( Config.ORDERER_RETRY_WAIT_TIME,                  config.getProperty( Config.ORDERER_RETRY_WAIT_TIME ));
    if( config.hasProperty( Config.ORDERER_WAIT_TIME                        )) System.setProperty( Config.ORDERER_WAIT_TIME,                        config.getProperty( Config.ORDERER_WAIT_TIME ));
    if( config.hasProperty( Config.PEER_EVENT_REGISTRATION_WAIT_TIME        )) System.setProperty( Config.PEER_EVENT_REGISTRATION_WAIT_TIME,        config.getProperty( Config.PEER_EVENT_REGISTRATION_WAIT_TIME ));
    if( config.hasProperty( Config.PEER_EVENT_RETRY_WAIT_TIME               )) System.setProperty( Config.PEER_EVENT_RETRY_WAIT_TIME,               config.getProperty( Config.PEER_EVENT_RETRY_WAIT_TIME ));

    if( config.hasProperty( Config.PEER_EVENT_RECONNECTION_WARNING_RATE )) System.setProperty( Config.PEER_EVENT_RECONNECTION_WARNING_RATE, config.getProperty( Config.PEER_EVENT_RECONNECTION_WARNING_RATE ));
    if( config.hasProperty( Config.GENESISBLOCK_WAIT_TIME               )) System.setProperty( Config.GENESISBLOCK_WAIT_TIME,               config.getProperty( Config.GENESISBLOCK_WAIT_TIME ));

    if( config.hasProperty( Config.MAX_LOG_STRING_LENGTH       )) System.setProperty( Config.MAX_LOG_STRING_LENGTH,       config.getProperty( Config.MAX_LOG_STRING_LENGTH ));
    if( config.hasProperty( Config.EXTRALOGLEVEL               )) System.setProperty( Config.EXTRALOGLEVEL,               config.getProperty( Config.EXTRALOGLEVEL  ));
    if( config.hasProperty( Config.LOGGERLEVEL                 )) System.setProperty( Config.LOGGERLEVEL,                 config.getProperty( Config.LOGGERLEVEL ));
    if( config.hasProperty( Config.DIAGNOTISTIC_FILE_DIRECTORY )) System.setProperty( Config.DIAGNOTISTIC_FILE_DIRECTORY, config.getProperty( Config.DIAGNOTISTIC_FILE_DIRECTORY ));

    if( config.hasProperty( Config.CONN_SSL_PROVIDER )) System.setProperty( Config.CONN_SSL_PROVIDER, config.getProperty( Config.CONN_SSL_PROVIDER ));
    if( config.hasProperty( Config.CONN_SSL_NEGTYPE  )) System.setProperty( Config.CONN_SSL_NEGTYPE,  config.getProperty( Config.CONN_SSL_NEGTYPE ));

    if( config.hasProperty( Config.CLIENT_THREAD_EXECUTOR_COREPOOLSIZE      )) System.setProperty( Config.CLIENT_THREAD_EXECUTOR_COREPOOLSIZE,      config.getProperty( Config.CLIENT_THREAD_EXECUTOR_COREPOOLSIZE ));
    if( config.hasProperty( Config.CLIENT_THREAD_EXECUTOR_MAXIMUMPOOLSIZE   )) System.setProperty( Config.CLIENT_THREAD_EXECUTOR_MAXIMUMPOOLSIZE,   config.getProperty( Config.CLIENT_THREAD_EXECUTOR_MAXIMUMPOOLSIZE ));
    if( config.hasProperty( Config.CLIENT_THREAD_EXECUTOR_KEEPALIVETIME     )) System.setProperty( Config.CLIENT_THREAD_EXECUTOR_KEEPALIVETIME,     config.getProperty( Config.CLIENT_THREAD_EXECUTOR_KEEPALIVETIME ));
    if( config.hasProperty( Config.CLIENT_THREAD_EXECUTOR_KEEPALIVETIMEUNIT )) System.setProperty( Config.CLIENT_THREAD_EXECUTOR_KEEPALIVETIMEUNIT, config.getProperty( Config.CLIENT_THREAD_EXECUTOR_KEEPALIVETIMEUNIT ));

    if( config.hasProperty( Config.PROPOSAL_CONSISTENCY_VALIDATION )) System.setProperty( Config.PROPOSAL_CONSISTENCY_VALIDATION, config.getProperty( Config.PROPOSAL_CONSISTENCY_VALIDATION ));
    if( config.hasProperty( Config.SERVICE_DISCOVER_FREQ_SECONDS   )) System.setProperty( Config.SERVICE_DISCOVER_FREQ_SECONDS,   config.getProperty( Config.SERVICE_DISCOVER_FREQ_SECONDS ));
    if( config.hasProperty( Config.SERVICE_DISCOVER_WAIT_TIME      )) System.setProperty( Config.SERVICE_DISCOVER_WAIT_TIME,      config.getProperty( Config.SERVICE_DISCOVER_WAIT_TIME ));

//    if( config.hasProperty( Config.LIFECYCLE_CHAINCODE_ENDORSEMENT_PLUGIN )) System.setProperty( Config.LIFECYCLE_CHAINCODE_ENDORSEMENT_PLUGIN, config.getProperty( Config.LIFECYCLE_CHAINCODE_ENDORSEMENT_PLUGIN ));
//    if( config.hasProperty( Config.LIFECYCLE_CHAINCODE_VALIDATION_PLUGIN  )) System.setProperty( Config.LIFECYCLE_CHAINCODE_VALIDATION_PLUGIN,  config.getProperty( Config.LIFECYCLE_CHAINCODE_VALIDATION_PLUGIN ));
//    if( config.hasProperty( Config.LIFECYCLE_INITREQUIREDDEFAULT          )) System.setProperty( Config.LIFECYCLE_INITREQUIREDDEFAULT,          config.getProperty( Config.LIFECYCLE_INITREQUIREDDEFAULT ));
  
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
