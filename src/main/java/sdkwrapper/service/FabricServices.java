package sdkwrapper.service;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hyperledger.fabric.sdk.Channel.DiscoveryOptions.createDiscoveryOptions;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.hyperledger.fabric.sdk.ChaincodeID;
import org.hyperledger.fabric.sdk.BlockListener;
import org.hyperledger.fabric.sdk.BlockchainInfo;
import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.CollectionConfigPackage;
import org.hyperledger.fabric.sdk.Channel.DiscoveryOptions;
import org.hyperledger.fabric.sdk.Channel.PeerOptions;
import org.hyperledger.fabric.sdk.CollectionConfigPackage.CollectionConfig;

import org.hyperledger.fabric.sdk.HFClient;
import org.hyperledger.fabric.sdk.Orderer;
import org.hyperledger.fabric.sdk.Peer;
import org.hyperledger.fabric.sdk.Peer.PeerRole;
import org.hyperledger.fabric.sdk.ProposalResponse;
import org.hyperledger.fabric.sdk.QueryByChaincodeRequest;
import org.hyperledger.fabric.sdk.ServiceDiscovery;
import org.hyperledger.fabric.sdk.TransactionProposalRequest;
import org.hyperledger.fabric.sdk.User;
import org.hyperledger.fabric.sdk.ChaincodeResponse.Status;
import org.hyperledger.fabric.sdk.exception.CryptoException;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.ProposalException;
import org.hyperledger.fabric.sdk.exception.ServiceDiscoveryException;
import org.hyperledger.fabric.sdk.exception.TransactionException;
import org.hyperledger.fabric.sdk.security.CryptoSuite;

import com.google.protobuf.InvalidProtocolBufferException;

import sdkwrapper.block.event.BlockEventFileStorePlayer;
import sdkwrapper.block.event.BlockEventsPlayerIF;
import sdkwrapper.config.OrgContextJsonParser;
import sdkwrapper.exceptions.BlockEventException;
import sdkwrapper.exceptions.ConfigurationException;
import sdkwrapper.exceptions.FabricRequestException;
import sdkwrapper.exceptions.FailedEndorsementException;
import sdkwrapper.exceptions.InfrastructureException;

import sdkwrapper.runtimemgr.RuntimeMgrIF;
import sdkwrapper.vo.config.ChainCodeInfo;
import sdkwrapper.vo.config.OrgContextVO;
import sdkwrapper.vo.config.OrgUserVO;
import sdkwrapper.vo.config.PeerVO;


public class FabricServices
{
  public static final String PORT_SEPARATOR        = ":";
  public static final int    MAX_PROPOSAL_ATTEMPTS = 3;
  
  private final  Logger logger         = LogManager.getLogger( FabricServices.class );
  private static long   invokeWaitTime = 120000;
  
  private RuntimeMgrIF runtimeMgr   = null;
  private HFClient     hfClient     = null;
  private OrgContextVO orgContext   = null;
  private Peer         orgPeer      = null;
  
  private Map<String, OrgUserVO>           users          = new HashMap<String, OrgUserVO>();
  private Collection<PeerVO>               discoveryPeers = new ArrayList<PeerVO>();
  private Collection<Peer>                 orgPeers       = new ArrayList<Peer>();
  private Map<String, Channel>             channels       = new HashMap<String, Channel>();
  private Map<String, ChainCodeInfo>       chainCodes     = new HashMap<String, ChainCodeInfo>();
  private Map<String, BlockEventsPlayerIF> listeners      = new HashMap<String, BlockEventsPlayerIF>();

  private Map<String, Map<String, CollectionConfig>> collections = new HashMap<String, Map<String, CollectionConfig>>();

  public HFClient                         getHFClient()       { return hfClient;       }
  public OrgContextVO                     getOrgContext()     { return orgContext;     }
  public Map<String, OrgUserVO>           getUsers()          { return users;          }
  public Collection<PeerVO>               getDiscoveryPeers() { return discoveryPeers; }
  public Collection<Peer>                 getOrgPeers()       { return orgPeers;       }
  public Map<String, Channel>             getChannels()       { return channels;       }
  public Map<String, ChainCodeInfo>       getChainCodes()     { return chainCodes;     }
  public Map<String, BlockEventsPlayerIF> getListeners()      { return listeners;      }
 
  public void setOrgContext( OrgContextVO context ) { this.orgContext = context; }
 
  
  
  public FabricServices( RuntimeMgrIF runMgr )
  {
    this.runtimeMgr = runMgr;
  }
  
  /**
   * Read the organization context json, parse into the domain objects (via OrgContextJsonParser), and initialize the 
   * channel connections via service discovery.
   * 
   * @param contextPath
   * @throws InfrastructureException
   * @throws ConfigurationException
   */
  public void initialize( String contextPath )
   throws InfrastructureException, ConfigurationException
  {
    logger.info( "Start organization initialization with context file = " + contextPath );
 
    // Initialize Fabric SDK Controller
    hfClient = HFClient.createNewInstance();
    try
    {
      hfClient.setCryptoSuite( CryptoSuite.Factory.getCryptoSuite() );
    } catch( CryptoException | InvalidArgumentException | IllegalAccessException | InstantiationException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e )
    {
      String msg = "Error initializing cryptosuite. Error = " + e.getMessage() + "; Fatal Error. Stopping";
      logger.error( msg );
      throw new InfrastructureException( msg );
    }

    // Load the Organization's context Json file
    OrgContextJsonParser.buildOrgContext( this, contextPath );
    
    logger.info( "Parsed context file = " + contextPath );

    // Load Default User Context into SDK Client
    try
    {
      hfClient.setUserContext( users.get( runtimeMgr.getConfig().getProperty( "admin.userid" )));
    } catch( InvalidArgumentException e )
    {
      String msg = "Error initializing fabric user context for admin user. Error = " + e.getMessage() + "; Fatal Error. Stopping";
      logger.error( msg );
      throw new ConfigurationException( msg );
    }

    // Build Discovery Peers which are the organization's peers and will be used for Eventing and Queries.
    for( PeerVO peerVO : discoveryPeers )
    {
      Peer peer = null;
      try
      {
        peer = buildDiscoveryPeer( peerVO );
        orgPeers.add( peer );
      } 
      catch( InvalidArgumentException e )
      {
        String msg = "Error initializing org Peer = " + peerVO.getPeerId() + "; Error = " + e.getMessage();
        logger.error( msg );
      }
    }
    
    if( orgPeers.isEmpty() )
    {
      String msg = "Error could not initialize any Peers for service discovery; Fatal Error. Stopping";
      logger.error( msg );
      throw new InfrastructureException( msg );
    }
    
    // Service Discovery
    String channelId = null;
    try
    {
      for( Peer peer : orgPeers )
      {
        Set<String> channelIds  = hfClient.queryChannels( peer );

        System.out.println( "channels found = " + channelIds );
        
        if( channelIds != null && !channelIds.isEmpty() )
        {
          for( String id : channelIds )
          {
            channelId = id;
            Channel discoveredChannel = discoverChannel( id, peer );

            channels.put( id, discoveredChannel );
          }
        }
        
        if( !channels.isEmpty() )
        {
          orgPeer = peer;
          break; // Dont need to go to the second one.
        }
      }
    }
    catch( InvalidArgumentException | ProposalException e )
    {
      String msg = "Error initializing service discovery channels for channel = " + channelId + "; Error = " + e.getMessage() + "; Fatal Error. Stopping";
      logger.error( msg );
      throw new InfrastructureException( msg );
    }
  }
  
  
  /**
   * Initialize the BlockEvent Listeners for each channel. 
   *  
   * @throws InfrastructureException
   */
  public void initializeListeners()
    throws BlockEventException
  {
    // Initialize Channel Block Event Listeners
    for( Map.Entry<String, Channel> entry : channels.entrySet() )
    {
      Channel channel = entry.getValue();
      createBlockListener( channel );
    }
  }
  
  public Map<String, Long> obtainBlockHeights()
    throws InfrastructureException
  {
    Map<String, Long> channelHeights = new HashMap<String, Long>();

    if( !channels.isEmpty() && orgPeer != null )
    {
      // Process each channel and obtain the current block height for the channel.
      for( Map.Entry<String, Channel> entry : channels.entrySet() )
      {
        Channel        channel = entry.getValue();
        BlockchainInfo info    = null;
            
        try
        {
          info = channel.queryBlockchainInfo( orgPeer );
        } 
        catch( ProposalException | InvalidArgumentException e )
        {
          String msg = "Error obtaining channel height for channel = " + channel.getName() + "; Error = " + e.getMessage() + "; Fatal Error. Stopping";
          logger.error( msg );
          throw new InfrastructureException( msg );
        }

        if( info != null )
        {
          channelHeights.put( channel.getName(), info.getHeight() );
        }
        else
        {
          String msg = "Error obtaining channel height for channel = " + channel.getName() + "; Error = received null BlockchainInfo; Fatal Error. Stopping";
          logger.error( msg );
          throw new InfrastructureException( msg );
        }
      }
    }
    
    return channelHeights;
  }
  
  
  /**
   * Send a Fabric Transaction to first the Endorsing Peers obtained via Service Discovery for the channel, check that the
   * endorsement policy is successful, and then send the transaction to the orderer for the channel.
   * 
   * @param channelId
   * @param userId
   * @param payload
   * @param methodName
   * @throws FabricRequestException
   * @throws FailedEndorsementException
   * @throws InfrastructureException
   */
  public void requestTransaction( String channelId, String userId, String[] payload, String methodName )
    throws FabricRequestException, FailedEndorsementException, InfrastructureException
  {
    Channel channel = validateRequestParms( channelId, userId, payload, methodName );
    
    String              ccId               = channel.getDiscoveredChaincodeNames().iterator().next();
    ChaincodeID.Builder chaincodeIDBuilder = ChaincodeID.newBuilder().setName( ccId );
    ChaincodeID         chainCodeId        = chaincodeIDBuilder.build();
     
    TransactionProposalRequest proposalRequest = hfClient.newTransactionProposalRequest();
    proposalRequest.setProposalWaitTime( invokeWaitTime );
    proposalRequest.setChaincodeID( chainCodeId );
    proposalRequest.setFcn(         methodName );
    proposalRequest.setArgs( payload );

    // If userId not found then we will use the User Context already set in hfClient
    if( users.containsKey( userId ))
    {
      proposalRequest.setUserContext( users.get( userId ));
    }  

    Collection<ProposalResponse> returnedEndorsements = null;
    DiscoveryOptions options = null;
    try
    {
      options = createDiscoveryOptions().setEndorsementSelector(ServiceDiscovery.EndorsementSelector.ENDORSEMENT_SELECTION_RANDOM );
    } catch( InvalidArgumentException e )
    {
      String errMsg = "Error creating service discovery options. Exception = " + e.getMessage();
      logger.error( errMsg );
      throw new FabricRequestException( errMsg );
    }
    
    int iter = 0;
    while( true )
    {
      try
      {
        returnedEndorsements = channel.sendTransactionProposalToEndorsers( proposalRequest, options );
        logger.info( "Endorsements returned for payload = " + payload );
        break;
      } 
      // Generated by invalid arguments. As we do not know the correct ones, throw an exception back to the invoking code.
      catch( InvalidArgumentException | ProposalException | ServiceDiscoveryException e )
      {
        // Generated by system error, generally connection. Attempt to reconnect and retry a max number of times.
        if( iter >= MAX_PROPOSAL_ATTEMPTS )
        {
          String errMsg = "MAX_PROPOSAL_ATTEMPTS for endorsement failed with exception = " + e.getMessage();
          logger.error( errMsg );
          throw new FabricRequestException( errMsg );
        }
        
        String errMsg = "Endorsement Exception. Exception = " + e.getMessage();
        logger.error( errMsg );

        e.printStackTrace();

        iter++;
        // Force channel shutdown, release all channel resources and then initialize a new sdk channel.
        restartChannel( channel );
      }
    }
    
    logger.info( "Returned Endorsements = " + returnedEndorsements.size() + " for payload = " + payload );
    // Determine whether the endorsement policy has been met via the policy associated with the chaincode.
    ChainCodeInfo ccInfo = getChainCode( ccId );
    if( ccInfo != null )
    {
      if( !ccInfo.getPolicy().isPolicyMet( returnedEndorsements ))
      {
        if( ccInfo.getPolicy().getFailedResponses() != null )
        {
          String errMsg = "Endorsement Failed. Found Failed Responses.";
          logger.error( errMsg );
          throw new FailedEndorsementException( ccInfo.getPolicy().getFailedResponses() );
        }
        else
        {
          String errMsg = "Endorsement Failed. Minimum responses not received.";
          logger.error( errMsg );
          throw new FailedEndorsementException( errMsg );
        }
      }
    }
    else
    {
      String msg = "Chaincode info = null. Not found for chaincode " + ccId;
      System.out.println( msg );
      logger.error( msg );
      throw new FabricRequestException( msg );
    }
 
    logger.info( "Sending ledger transaction to orderer." );
    try
    {
      // Note we are not capturing the CompleteableFuture returned here as it is only Completed for the transaction after.
      // the BlockEvent is returned from the Peer. Instead we are listening for and processing the Block Events.
      channel.sendTransaction( ccInfo.getPolicy().getSuccessfulResponses(), channel.getOrderers() );
    } 
    catch( Exception e )
    {
      // All purpose catch for transport errors which are passed thru.
      String errMsg = "Error sending transaction to orderer. Error = " + e.getMessage();
      logger.error( errMsg );
      throw new FabricRequestException( errMsg );
    }
  }

  /**
   * Send a Fabric Transaction with private Data to first the Endorsing Peers obtained via Service Discovery for the channel, check that the
   * endorsement policy is successful, and then send the transaction to the orderer for the channel.
   * 
   * @param channelId
   * @param userId
   * @param payload
   * @param methodName
   * @throws FabricRequestException
   * @throws FailedEndorsementException
   * @throws InfrastructureException
   */
  public void requestTransactionWithPrivateData( String channelId, String userId, String[] payload, Map<String, byte[]> privateDataMap, String collectionId, String methodName )
    throws FabricRequestException, FailedEndorsementException, InfrastructureException
  {
    Channel channel = validateRequestParms( channelId, userId, payload, methodName );
    
    String              ccId               = channel.getDiscoveredChaincodeNames().iterator().next();
    ChaincodeID.Builder chaincodeIDBuilder = ChaincodeID.newBuilder().setName( ccId );
    ChaincodeID         chainCodeId        = chaincodeIDBuilder.build();
     
    TransactionProposalRequest proposalRequest = hfClient.newTransactionProposalRequest();
    proposalRequest.setProposalWaitTime( invokeWaitTime );
    proposalRequest.setChaincodeID( chainCodeId );
    proposalRequest.setFcn(         methodName );
    proposalRequest.setArgs( payload );

    // Private data needs to be sent via Transient field to prevent identifiable information being sent to the orderer.
    try
    {
      proposalRequest.setTransientMap( privateDataMap );
    } catch( InvalidArgumentException e1 )
    {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }

    // Validate private collection on channel
    boolean inChannel = false;
    if( collections.containsKey( channelId ))
    {
      Map<String, CollectionConfig> collectionConfigs = collections.get( channelId );
      if( collectionConfigs.containsKey( collectionId ))
      {
        inChannel = true;
      }
    }

    if( !inChannel )
    {
      String errMsg = "Requested private Data collection not in requested channel.";
      logger.error( errMsg );
      throw new FabricRequestException( errMsg );
    }
    
    // If userId not found then we will use the User Context already set in hfClient
    if( users.containsKey( userId ))
    {
      proposalRequest.setUserContext( users.get( userId ));
    }  

    Collection<ProposalResponse> returnedEndorsements = null;
    DiscoveryOptions options = null;
    try
    {
      options = createDiscoveryOptions().setEndorsementSelector( ServiceDiscovery.EndorsementSelector.ENDORSEMENT_SELECTION_RANDOM );
    } catch( InvalidArgumentException e )
    {
      String errMsg = "Error creating service discovery options. Exception = " + e.getMessage();
      logger.error( errMsg );
      throw new FabricRequestException( errMsg );
    }

    int iter = 0;
    while( true )
    {
      try
      {
        returnedEndorsements = channel.sendTransactionProposalToEndorsers( proposalRequest, options );
        logger.info( "Endorsements returned for payload = " + payload );
        break;
      } 
      // Generated by invalid arguments. As we do not know the correct ones, throw an exception back to the invoking code.
      catch( InvalidArgumentException | ProposalException | ServiceDiscoveryException e )
      {
        // Generated by system error, generally connection. Attempt to reconnect and retry a max number of times.
        if( iter >= MAX_PROPOSAL_ATTEMPTS )
        {
          String errMsg = "MAX_PROPOSAL_ATTEMPTS for endorsement failed with exception = " + e.getMessage();
          logger.error( errMsg );
          throw new FabricRequestException( errMsg );
        }
        
        String errMsg = "Endorsement Exception. Exception = " + e.getMessage();
        logger.error( errMsg );

        e.printStackTrace();

        iter++;
        // Force channel shutdown, release all channel resources and then initialize a new sdk channel.
        restartChannel( channel );
      }
    }
    
    logger.info( "Returned Endorsements = " + returnedEndorsements.size() + " for payload = " + payload );
    // Determine whether the endorsement policy has been met via the policy associated with the chaincode.
    ChainCodeInfo ccInfo = getChainCode( ccId );
    if( ccInfo != null )
    {
      if( !ccInfo.getPolicy().isPolicyMet( returnedEndorsements ))
      {
        if( ccInfo.getPolicy().getFailedResponses() != null )
        {
          String errMsg = "Endorsement Failed. Found Failed Responses.";
          logger.error( errMsg );
          throw new FailedEndorsementException( ccInfo.getPolicy().getFailedResponses() );
        }
        else
        {
          String errMsg = "Endorsement Failed. Minimum responses not received.";
          logger.error( errMsg );
          throw new FailedEndorsementException( errMsg );
        }
      }
    }
    else
    {
      String msg = "Chaincode info = null. Not found for chaincode " + ccId;
      System.out.println( msg );
      logger.error( msg );
      throw new FabricRequestException( msg );
    }
 
    logger.info( "Sending ledger transaction to orderer." );
    try
    {
      // Note we are not capturing the CompleteableFuture returned here as it is only Completed for the transaction after.
      // the BlockEvent is returned from the Peer. Instead we are listening for and processing the Block Events.
      channel.sendTransaction( ccInfo.getPolicy().getSuccessfulResponses(), channel.getOrderers() );
    } 
    catch( Exception e )
    {
      // All purpose catch for transport errors which are passed thru.
      String errMsg = "Error sending transaction to orderer. Error = " + e.getMessage();
      logger.error( errMsg );
      throw new FabricRequestException( errMsg );
    }
  }

  /**
   * Query channel world state.
   * 
   * @param channelId
   * @param userId
   * @param args
   * @param method
   * @return
   * @throws FabricRequestException
   */
  public String query( String channelId, String userId, String[] args, String method )
    throws FabricRequestException
  {
    Channel channel = validateRequestParms( channelId, userId, args, method );
    
    String              ccId               = channel.getDiscoveredChaincodeNames().iterator().next();
    ChaincodeID.Builder chaincodeIDBuilder = ChaincodeID.newBuilder().setName( ccId );
    ChaincodeID         chainCodeId        = chaincodeIDBuilder.build();
    
    QueryByChaincodeRequest queryByChaincodeRequest = hfClient.newQueryProposalRequest();
    queryByChaincodeRequest.setArgs( args ); // test using bytes as args. End2end uses Strings.
    queryByChaincodeRequest.setFcn( method );
    queryByChaincodeRequest.setChaincodeID( chainCodeId );

    Collection<ProposalResponse> queryProposals;
    
    for( Peer peer : orgPeers )
    {
      try 
      {
        Collection<Peer> queryPeer = new ArrayList<Peer>();
        queryPeer.add( peer );
        queryProposals = channel.queryByChaincode( queryByChaincodeRequest, queryPeer );

        for( ProposalResponse proposalResponse : queryProposals ) 
        {
          if (!proposalResponse.isVerified() || proposalResponse.getStatus() != Status.SUCCESS) 
          {
            String msg = "Failed query proposal from peer " + proposalResponse.getPeer().getName() + " status: " + proposalResponse.getStatus() +
                        ". Messages: " + proposalResponse.getMessage()
                        + ". Was verified : " + proposalResponse.isVerified();
            logger.error( msg );
          }
          else
          {
            return proposalResponse.getProposalResponse().getResponse().getPayload().toStringUtf8();
          }
        }
      }
      catch( Exception e ) 
      {
        throw new FabricRequestException( e.getMessage() );
      }
    }
    
    final String msg = "No Org Peer provided a valid query response for query params " + args;
    logger.error( msg );
    throw new FabricRequestException( msg );
  }
 
  /**
   * Query channel world state and Private Data.
   * 
   * @param channelId
   * @param userId
   * @param collectionId
   * @param args
   * @param privateDataMap
   * @param method
   * @return
   * @throws FabricRequestException
   */
  public String queryWithPrivateData( String channelId, String userId, String collectionId, String[] args, Map<String, byte[]> privateDataMap, String method )
    throws FabricRequestException
  {
    Channel channel = validateRequestParms( channelId, userId, args, method );
    
    String              ccId               = channel.getDiscoveredChaincodeNames().iterator().next();
    ChaincodeID.Builder chaincodeIDBuilder = ChaincodeID.newBuilder().setName( ccId );
    ChaincodeID         chainCodeId        = chaincodeIDBuilder.build();
    
    QueryByChaincodeRequest queryRequest = hfClient.newQueryProposalRequest();
    queryRequest.setArgs( args ); // test using bytes as args. End2end uses Strings.
    queryRequest.setFcn( method );
    queryRequest.setChaincodeID( chainCodeId );

    // Private data needs to be sent via Transient field to prevent identifiable information being sent to the orderer.
    try
    {
      queryRequest.setTransientMap( privateDataMap );
    } catch( InvalidArgumentException e1 )
    {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }

    // Validate private collection on channel
    boolean inChannel = false;
    if( collections.containsKey( channelId ))
    {
      Map<String, CollectionConfig> collectionConfigs = collections.get( channelId );
      if( collectionConfigs.containsKey( collectionId ))
      {
        inChannel = true;
      }
    }

    if( !inChannel )
    {
      String errMsg = "Requested private Data collection not in requested channel.";
      logger.error( errMsg );
      throw new FabricRequestException( errMsg );
    }

    Collection<ProposalResponse> queryProposals;
    
    for( Peer peer : orgPeers )
    {
      try 
      {
        Collection<Peer> queryPeer = new ArrayList<Peer>();
        queryPeer.add( peer );
        queryProposals = channel.queryByChaincode( queryRequest, queryPeer );

        for( ProposalResponse proposalResponse : queryProposals ) 
        {
          if (!proposalResponse.isVerified() || proposalResponse.getStatus() != Status.SUCCESS) 
          {
            String msg = "Failed query proposal from peer " + proposalResponse.getPeer().getName() + " status: " + proposalResponse.getStatus() +
                        ". Messages: " + proposalResponse.getMessage()
                        + ". Was verified : " + proposalResponse.isVerified();
            logger.error( msg );
          }
          else
          {
            return proposalResponse.getProposalResponse().getResponse().getPayload().toStringUtf8();
          }
        }
      }
      catch( Exception e ) 
      {
        throw new FabricRequestException( e.getMessage() );
      }
    }
    
    final String msg = "No Org Peer provided a valid query response for query params " + args;
    logger.error( msg );
    throw new FabricRequestException( msg );
  }

  
  private Channel validateRequestParms( String channelId, String userId, String[] payload, String methodName )
    throws FabricRequestException
  {
    if( payload == null )
    {
      String errMsg = "Tran payload = null. Payload is required.";
      logger.error( errMsg );
      throw new FabricRequestException( errMsg );
    }
    
    if( userId == null )
    {
      String errMsg = "Proposal request did not contain required userId.";
      logger.error( errMsg );
      throw new FabricRequestException( errMsg );
    }

    if( channelId == null )
    {
      String errMsg = "Proposal request did not contain required channelId.";
      logger.error( errMsg );
      throw new FabricRequestException( errMsg );
    }

    Channel channel = channels.get( channelId );
    if( channel == null )
    {
      String errMsg = "Proposal request channelId is invalid.";
      logger.error( errMsg );
      throw new FabricRequestException( errMsg );
    }
    
    if( users.containsKey( userId ))
    {
      User user = users.get( userId );
      try
      {
        hfClient.setUserContext( user );
      } catch( InvalidArgumentException e )
      {
        String errMsg = "Transaction request setting userId error. Error = " + e.getMessage();
        logger.error( errMsg );
        throw new FabricRequestException( errMsg );
      }
    }
    else
    {
      String errMsg = "Transaction request userId not found.";
      logger.error( errMsg );
      throw new FabricRequestException( errMsg );
    }
    
    return channel;
  }
  
  public OrgUserVO getUser( String id )
  {
    if( users.containsKey( id ))
      return users.get( id );
    
    return null;
  }
  
  /**
   * Obtain the chaincode associated with the id parameter.
   * 
   * @param id
   * @return
   */
  public ChainCodeInfo getChainCode( String id )
  {
    if( chainCodes.containsKey( id ))
      return chainCodes.get( id );
    
    return null;
  }

  
  /**
   * Set a Block Event Listener on the channel for receiving Block Events.
   * 
   * @param channel
   * @param listener
   * @throws InvalidArgumentException
   */
  public void setBlockListener( Channel channel, BlockListener listener )
   throws InvalidArgumentException
  {
    channel.registerBlockListener( listener );
  }


  private void createBlockListener( Channel channel )
    throws BlockEventException
  {
    if( channel == null )
      throw new BlockEventException( "Error null channel." );

    BlockEventsPlayerIF eventsPlayer = new BlockEventFileStorePlayer();
    eventsPlayer.initializeEventing( runtimeMgr, channel, orgPeer );
    listeners.put( channel.getName(), eventsPlayer );
  }


  private boolean isValidDiscovery( Channel channel )
  {
    EnumSet<PeerRole> endorserSet = EnumSet.of( PeerRole.ENDORSING_PEER );

    if( channel.getDiscoveredChaincodeNames() != null && !channel.getDiscoveredChaincodeNames().isEmpty() &&
        channel.getPeers()                    != null && !channel.getPeers().isEmpty() &&
        channel.getPeers( endorserSet )       != null && !channel.getPeers( endorserSet ).isEmpty() &&
        channel.getOrderers()                 != null && !channel.getOrderers().isEmpty() )
    {
      return true;
    }
    
    return false;
  }
  
  private Peer buildDiscoveryPeer( PeerVO peerVO ) 
   throws InvalidArgumentException, ConfigurationException
  {
    // Build and load the Fabric Peers which will be used for Service Discovery
    // Build Peer Properties file. Valid properties are:
    //  pemfile - File location for x509 pem certificate for SSL
    //  trustServerCertificate - boolean override CN to match pemfile certificate - for dev only.
    //  hostnameOverride - Specify the certificates CN - for dev only
    //  sslProvider - Specify the SSL provider - 'openSSL' or 'JDK'
    //  negotiationType - Specify the type of negotiation - TLS or planText
    Properties props = new Properties();
    props.put( "pemBytes", peerVO.getTlsCertificate().getBytes() );
    props.put( "trustServerCertificate", peerVO.isTrustServerCert() );
    props.put( "sslProvider", peerVO.getSslProvider() );
    props.put( "negotiationType", peerVO.getNegotiationType() );
      
    // Note - Service Discovery uses this pattern as the Peer id
    final String peerId = peerVO.getIpAddress() + ":" + peerVO.getEndorsePort();
    return hfClient.newPeer( peerId, peerVO.getEndorseUrl(), props );
  }

  
  private Channel discoverChannel( String channelId, Peer peer )
    throws InfrastructureException
  {
    Channel discoveredChannel = null;
    try
    {
      discoveredChannel = hfClient.newChannel( channelId );
    } 
    catch( InvalidArgumentException e )
    {
      String errMsg = "Error initializing new channel via service discovery for channel = " + channelId + ". Error = " + e.getMessage();
      logger.error( errMsg );
      throw new InfrastructureException( errMsg );
    } 

    logger.info( "Created new channel " + discoveredChannel.getName() );

    Properties sdprops = new Properties();
    PeerVO     peerVO  = getPeerVO( peer.getName() );
    if( peerVO == null )
    {
      throw new InfrastructureException( "Discovery Peer info could not be found for organization Peer." );
    }
    
    // Obtain the last processed Block Event for this channel.
    long seqStart = 0;
    try
    {
      seqStart = runtimeMgr.getBlockSeqStore().getCurrentSequenceNumber( channelId ) + 1;
    } 
    catch( BlockEventException e1 )
    {
      throw new InfrastructureException( "Error obtaining the last block processed for channel = " + channelId );
    }
    
    try
    {
      PeerOptions options = Channel.PeerOptions.createPeerOptions().setPeerRoles(EnumSet.of( PeerRole.SERVICE_DISCOVERY, PeerRole.LEDGER_QUERY, PeerRole.EVENT_SOURCE, PeerRole.CHAINCODE_QUERY, PeerRole.ENDORSING_PEER ));
      options = options.startEvents( seqStart );
      discoveredChannel.addPeer( peer, options );
    } 
    catch( InvalidArgumentException e )
    {
      String errMsg = "Error adding discovery peer to channel for channel = " + channelId + ". Error = " + e.getMessage();
      logger.error( errMsg );
      throw new InfrastructureException( errMsg );
    }

    // Need to provide client TLS certificate and key files when running mutual tls.
    if( peerVO.getUseMutualTLS() )
    {
      sdprops.put("org.hyperledger.fabric.sdk.discovery.default.clientCertFile", "src/test/fixture/sdkintegration/e2e-2Orgs/v1.2/crypto-config/peerOrganizations/org1.example.com/users/User1@org1.example.com/tls/client.crt");
      sdprops.put("org.hyperledger.fabric.sdk.discovery.default.clientKeyFile", "src/test/fixture/sdkintegration/e2e-2Orgs/v1.2/crypto-config/peerOrganizations/org1.example.com/users/User1@org1.example.com/tls/client.key");
    } 
    
    if ( !peerVO.getUseTLS() ) 
    {
      sdprops.put("org.hyperledger.fabric.sdk.discovery.default.protocol", "grpc:");
    }
    
    discoveredChannel.setServiceDiscoveryProperties(sdprops);

    try
    {
      discoveredChannel.initialize(); // initialize the channel.

      if( isValidDiscovery( discoveredChannel ))
      {
        for( Peer cPeer : discoveredChannel.getPeers() )
        {
          String msg = "Channel " + discoveredChannel.getName() + " added Peer " + cPeer.getName();
          System.out.println( msg );
          logger.info( msg );
        }
        
        for( Orderer orderer : discoveredChannel.getOrderers() )
        {
          String msg = "Channel " + discoveredChannel.getName() + " added Orderer " + orderer.getName();
          System.out.println( msg );
          logger.info( msg );
        }

        try
        {
          for( String ccId : discoveredChannel.getDiscoveredChaincodeNames() )
          {
            CollectionConfigPackage queryCollections = discoveredChannel.queryCollectionsConfig( ccId, peer, users.get( "Admin" ));
          
            if( queryCollections != null && !queryCollections.getCollectionConfigs().isEmpty() )
            {
              Map<String, CollectionConfig> ccCollections = new HashMap<String, CollectionConfig>();
            
              for( CollectionConfig collectionConfig : queryCollections.getCollectionConfigs() ) 
              {
                ccCollections.put( collectionConfig.getName(), collectionConfig );
              }
            
              collections.put( ccId, ccCollections );
            }
          }
        }
        catch( ProposalException | InvalidProtocolBufferException e )
        {
          String msg = "Error encountered during service discovery for channel " + channelId + ". Error = " + e.getMessage() + "; Fatal Error. Stopping";
          logger.error( msg );
          throw new InfrastructureException( msg );
        }
        
        logger.info( "Channel = " + channelId + " successfully initialized via Service Discovery." );
        return discoveredChannel;
      }
      else
      {
        String msg = "Error encountered during service discovery for channel " + channelId + ". Error = invalid discovery information.";
        logger.error( msg );
        throw new InfrastructureException( msg );
      }
    }
    catch( InvalidArgumentException | TransactionException e )
    {
      String msg = "Error encountered during service discovery for channel " + channelId + ". Error = " + e.getMessage() + "; Fatal Error. Stopping";
      logger.error( msg );
      throw new InfrastructureException( msg );
    }
  }
  
  
  private void restartChannel( Channel channel )
   throws InfrastructureException
  {
    channel.shutdown( true );
    
    // Stop current block event listener on the channel
    if( listeners.containsKey( channel.getName() ))
    {
      BlockEventsPlayerIF eventer = listeners.get( channel.getName() );
      
      try
      {
        eventer.shutdown();
      } 
      catch( BlockEventException e )
      {
        String msg = "Error encountered shutting down Block Events. Error = " + e.getMessage();
        logger.error( msg );
        throw new InfrastructureException( msg );
      }
 
      listeners.remove( channel.getName() );
    }
    
    // Service Discovery
    Channel newChannel = null;
    String  msg        = null;
    
    for( Peer peer : orgPeers )
    {
      try
      {
        newChannel = discoverChannel( channel.getName(), peer );
        channels.put( channel.getName(), newChannel );

        createBlockListener( newChannel );
      }
      catch( InfrastructureException | BlockEventException e )
      {
        msg = "Error restarting channel = " + newChannel.getName() + "; Error = " + e.getMessage();
        logger.error( msg );
      }
      
      if( newChannel != null && listeners.containsKey( newChannel.getName() ))
      {
        break; // Dont need to go to the next peer.  
      }
    }
    
    if( msg != null )
    {
      throw new InfrastructureException( msg );
    }
  }
  
  private PeerVO getPeerVO( String id )
  {
    for( PeerVO peerVO : discoveryPeers )
    {
      if( id.compareTo( peerVO.getPeerId() ) == 0 || id.compareTo( peerVO.getIpAddress() + ":" + peerVO.getEndorsePort() ) == 0 )
        return peerVO;
    }
    
    return null;
  }
}
