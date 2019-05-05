package sdkwrapper.config;

import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import sdkwrapper.exceptions.ConfigurationException;
import sdkwrapper.fabric.policy.EndorsementPolicyIF;
import sdkwrapper.service.FabricServices;
import sdkwrapper.vo.config.ChainCodeInfo;
import sdkwrapper.vo.config.ChainCodeVO;
import sdkwrapper.vo.config.OrgContextVO;
import sdkwrapper.vo.config.OrgUserVO;
import sdkwrapper.vo.config.PeerVO;

public class OrgContextJsonParser
{
  private static final Logger logger = LogManager.getLogger( OrgContextJsonParser.class );

  public static void buildOrgContext( FabricServices service, String configPath )
   throws ConfigurationException
  {
    Gson             gson     = new Gson();
    Type             ORG_TYPE = new TypeToken<ServiceDiscovery>() {}.getType();
    ServiceDiscovery context  = null;
    

    logger.info( "OrgContextJsonParser.buildOrgContext starting with path " + configPath );

    FileReader in     = null;
    JsonReader reader = null;
    try
    {
      in     = new FileReader( configPath );
      reader = new JsonReader( in );
      
      //convert the json to ServiceDiscovery
      context = gson.fromJson( reader, ORG_TYPE );
      logger.info( "OrgContextJsonParser.buildOrgContext parsed json file." );
    } catch( IOException eIO )
      {
        String err = "Error processing config json file. Error = " + eIO.getMessage() + "; Fatal error. Stopping service.";
        System.out.println( err );
        logger.error(       err );
        
        throw new ConfigurationException( err );
      }
      finally
      {
        try
        {
          in.close();
          reader.close();
        } catch( IOException e )
        {
          // do nothing - although potentially this is a small memory leak. nothing we really can do
        }
      }
    
    if( context == null )
    {
      final String msg = "Unable to parse Organization Context json. Fatal error. Stopping service.";
      logger.error( msg );
      throw new ConfigurationException( msg );
    }
    
    initializeClientSdk( service, context );

    logger.info( "OrgContextJsonParser.buildOrgContext Complete." );
  }

  private static void initializeClientSdk( FabricServices service, ServiceDiscovery context )
   throws ConfigurationException
  {
    logger.info( "OrgContextJsonParser.loadService starting." );

    // Validate required configuration
    if( context.getOrgContext()              == null ) throw new ConfigurationException( "Org context header information not found." );
    if( context.getOrgContext().getOrgId()   == null ) throw new ConfigurationException( "Org Id is required and not found." );
    if( context.getOrgContext().getOrgName() == null ) throw new ConfigurationException( "Org Name is required and not found." );
    if( context.getOrgContext().getMspId()   == null ) throw new ConfigurationException( "Org MspId is required and not found" );

    if( context.getChainCodes() == null || context.getChainCodes().isEmpty() ) 
      throw new ConfigurationException( "No chaincodes have been configured. Fatal error. Stopping service." );

    if( context.getUsers() == null || context.getUsers().isEmpty() )
      throw new ConfigurationException( "No Users have been configured. Fatal error. Stopping service." );

    logger.info( "OrgContextJsonParser.loadService passed initial validation." );

    // Now we can load the Domain objects for the Fabric Wrapper and SDK
    OrgContextVO orgContext = new OrgContextVO();
 
    // Load Org Context Header Attributes
    orgContext.setOrgId(   context.getOrgContext().getOrgId()   );
    orgContext.setOrgName( context.getOrgContext().getOrgName() );
    orgContext.setMspId(   context.getOrgContext().getMspId()   );
    orgContext.setMspPath( context.getOrgContext().getMspPath() );
    
    service.setOrgContext( orgContext );

    logger.info( "OrgContextJsonParser.loadService loaded OrgContextVO to the service." );

    // Process Chaincodes
    for( ChainCodeVO code : context.getChainCodes() )
    {
      if( code == null || code.getEndorsementPolicyName() == null )
      {
        final String msg = "No Endorsement Policy found for " + code.getChainCodeName() + "; Fatal Error. Stopping service.";
        logger.error( msg );
        throw new ConfigurationException( msg );
      }
      
      ChainCodeInfo info = new ChainCodeInfo( code, instantiatePolicy( code.getEndorsementPolicyName() ));
      
      service.getChainCodes().put( code.getChainCodeName(), info );
    }

    logger.info( "OrgContextJsonParser.loadService loaded ChainCodes to the service." );

    // Process Users
    String userId = null;
    try
    {
      for( UserInfo user : context.getUsers() )
      {
        userId = user.getName();
        
        OrgUserVO orgUser = new OrgUserVO();
      
        orgUser.setName(          user.getName()  );
        orgUser.setRoles(         user.getRoles()    );
        orgUser.setAccount(       user.getAccount() );
        orgUser.setAffiliation(   user.getAffiliation()  );
        orgUser.setMspId(         user.getMspId()     );
        orgUser.setEnrollPrivKey( keyFromString( user.getEnrollPrivKey() ));
        orgUser.setEnrollCert(    encodedString( user.getEnrollCert()    ));
        orgUser.setEnrollPubKey(  user.getEnrollPubKey() );

        service.getUsers().put( user.getName(), orgUser );
      }
    } 
    catch( NoSuchAlgorithmException | InvalidKeySpecException | IOException e )
    {
      final String msg = "Error obtaining User Private Key or Certificate for user = " + userId + ". Error = " + e.getMessage() + "; Fatal error. Stopping service."; 
      logger.error( msg );
      throw new ConfigurationException( msg );
    }
    
    logger.info( "OrgContextJsonParser.loadService loaded OrgUserVO map to the service." );

    if( context.getPeers() != null && !context.getPeers().isEmpty() )
    {
      for( PeerInfo peerInfo : context.getPeers() )
      {
        service.getDiscoveryPeers().add( buildPeerVO( peerInfo ));
      }
      
      for( PeerVO peerVO : service.getDiscoveryPeers() )
      {
        logger.info( "Discovery Peer found = " + peerVO.getPeerId() );
      }
    }
    else
    {
      final String msg = "No initial service discovery peers found. Configuration invalid. Stopping service.";
      logger.error( msg );
      throw new ConfigurationException( msg );
    }
  }
  
  private static PeerVO buildPeerVO( PeerInfo peerInfo )
  {
    PeerVO peerVO = new PeerVO();
    
    peerVO.setPeerId(          peerInfo.getPeerId()          );
    peerVO.setOrgId(           peerInfo.getOrgId()           );
    peerVO.setIpAddress(       peerInfo.getIpAddress()       );
    peerVO.setOrgEventHub(     peerInfo.isOrgEventHub()      );
    peerVO.setEndorsePort(     peerInfo.getEndorsePort()     );
    peerVO.setEventingPort(    peerInfo.getEventingPort()    );
    peerVO.setTlsCertificate(  peerInfo.getTlsCertificate()  );
    peerVO.setTrustServerCert( peerInfo.isTrustServerCert()  );
    peerVO.setSslProvider(     peerInfo.getSslProvider()     );
    peerVO.setNegotiationType( peerInfo.getNegotiationType() );
    peerVO.setUseTLS(          peerInfo.isUseTLS()           );
    peerVO.setUseMutualTLS(    peerInfo.isUseMutualTLS()     );
    
    return peerVO;
  }
  
  private static EndorsementPolicyIF instantiatePolicy( String className )
   throws ConfigurationException
  {
    try
    {
      return (EndorsementPolicyIF) Class.forName( className ).newInstance();
    } 
    catch( IllegalAccessException eI )
    {
      throw new ConfigurationException( "Error Instantiating Endorsement Policy - " + className + ". Error = " + eI.getMessage() );
    }
    catch( InstantiationException eIn )
    {
      throw new ConfigurationException( "Error Instantiating Endorsement Policy - " + className + ". Error = " + eIn.getMessage() );
    }
    catch( ClassNotFoundException eC )
    {
      throw new ConfigurationException( "Class Not Found - Endorsement Policy - " + className + ". Error = " + eC.getMessage() );
    }
  }
  
  private static PrivateKey keyFromString( String key ) 
   throws NoSuchAlgorithmException, InvalidKeySpecException, IOException
  {
    byte[] keyBytes = new String( key.getBytes( Charset.forName( "UTF-8" ))).getBytes();

    final PEMParser pemParser = new PEMParser( new StringReader( new String( keyBytes )));

    PrivateKeyInfo pemPair   = (PrivateKeyInfo) pemParser.readObject();
    PrivateKey    privateKey = new JcaPEMKeyConverter().getPrivateKey(pemPair);

    return privateKey;
  }

  private static String encodedString( String cert ) 
   throws UnsupportedEncodingException
  {
    return new String( cert.getBytes( Charset.forName( "UTF-8" )));
  }

  /**
   * Temporary Class for Service Discovery
   */
  class ServiceDiscovery
  {
    OrgContextVO      orgContext;
    List<ChainCodeVO> chainCodes;
    List<UserInfo>    users;
    List<PeerInfo>    peers;
    
    public OrgContextVO      getOrgContext() { return orgContext; }
    public List<ChainCodeVO> getChainCodes() { return chainCodes; }
    public List<UserInfo>    getUsers()      { return users;      }
    public List<PeerInfo>    getPeers()      { return peers;      }
    
    public void setOrgContext( OrgContextVO      org   ) { this.orgContext = org;   }
    public void setChainCodes( List<ChainCodeVO> codes ) { this.chainCodes = codes; }
    public void setUsers(      List<UserInfo>    users ) { this.users      = users; }
    public void setPeers(      List<PeerInfo>    peers ) { this.peers      = peers; }
  }
  
  
  /**
   * Temporary object for json parsing
   * @author tim
   *
   */
  class OrgContext
  {
    OrgContextVO      orgContext;
    List<ChainCodeVO> chainCodes;
    List<UserInfo>    users;
    List<ChannelInfo> channels;
    
    public OrgContextVO      getOrgContext() { return orgContext; }
    public List<ChainCodeVO> getChainCodes() { return chainCodes; }
    public List<UserInfo>    getUsers()      { return users;      }
    public List<ChannelInfo> getChannels()   { return channels;   }
    
    public void setOrgContext( OrgContextVO      org      ) { this.orgContext = org;      }
    public void setChainCodes( List<ChainCodeVO> codes    ) { this.chainCodes = codes;    }
    public void setUsers(      List<UserInfo>    users    ) { this.users      = users;    }
    public void setChannels(   List<ChannelInfo> channels ) { this.channels   = channels; }
  }

  class UserInfo
  {
    private String      name = null;
    private Set<String> roles       = new HashSet<String>();
    private String      account     = null;
    private String      affiliation = null;
    private String      mspId       = null;
    
    private String      enrollPrivKey = null;  // 
    private String      enrollCert    = null;
    private String      enrollPubKey  = null;  // Not Used
   
    public String      getName()          { return name;          }
    public Set<String> getRoles()         { return roles;         }
    public String      getAccount()       { return account;       }
    public String      getAffiliation()   { return affiliation;   }
    public String      getMspId()         { return mspId;         }
    public String      getEnrollPrivKey() { return enrollPrivKey; }
    public String      getEnrollCert()    { return enrollCert;    }
    public String      getEnrollPubKey()  { return enrollPubKey;  }
    public String      getMSPID()         { return mspId;         }
    
    public void setName(          String      name          ) { this.name          = name;          }
    public void setRoles(         Set<String> roles         ) { this.roles         = roles;         }
    public void setAccount(       String      account       ) { this.account       = account;       }
    public void setAffiliation(   String      affiliation   ) { this.affiliation   = affiliation;   }
    public void setMspId(         String      mspId         ) { this.mspId         = mspId;         }
    public void setEnrollPrivKey( String      enrollPrivKey ) { this.enrollPrivKey = enrollPrivKey; }
    public void setEnrollCert(    String      enrollCert    ) { this.enrollCert    = enrollCert;    }
    public void setEnrollPubKey(  String      enrollPubKey  ) { this.enrollPubKey  = enrollPubKey;  } 
  }
  
  
  /**
   * Temporary object for json parsing
   * @author tim
   *
   */
  class ChannelInfo
  {
    String channelName;
    String channelType;
    int    invokeWaitTime = 0;
    int    deployWaitTime = 0;
    int    gossipWaitTime = 0;
    private Collection<PeerInfo>    endorserPeers  = new ArrayList<PeerInfo>();
    private Collection<OrdererInfo> ordererNodes   = new ArrayList<OrdererInfo>();
    private Collection<PeerInfo>    eventHubs      = new ArrayList<PeerInfo>();
    
    public String                  getChannelName()    { return channelName;    }
    public String                  getChannelType()    { return channelType;    }
    public int                     getInvokeWaitTime() { return invokeWaitTime; }
    public int                     getDeployWaitTime() { return deployWaitTime; }
    public int                     getGossipWaitTime() { return gossipWaitTime; }
    public Collection<PeerInfo>    getEndorserPeers()  { return endorserPeers;  }
    public Collection<OrdererInfo> getOrdererNodes()   { return ordererNodes;   }
    public Collection<PeerInfo>    getEventHubs()      { return eventHubs;      }
    
    public void setChannelName(    String name           ) { this.channelName    = name; }
    public void setChannelType(    String type           ) { this.channelType    = type; }
    public void setInvokeWaitTime( int    invokeWaitTime ) { this.invokeWaitTime = invokeWaitTime; }
    public void setDeployWaitTime( int    deployWaitTime ) { this.deployWaitTime = deployWaitTime; }
    public void setGossipWaitTime( int    gossipWaitTime ) { this.gossipWaitTime = gossipWaitTime; }

    public void setEndorserPeers(  List<PeerInfo>    peers ) { this.endorserPeers = peers; }
    public void setOrdererNodes(   List<OrdererInfo> nodes ) { this.ordererNodes  = nodes; }
    public void setEventHubs(      List<PeerInfo>    hubs  ) { this.eventHubs     = hubs;  }
  }
  
  /**
   * Temporary object for json parsing
   * @author tim
   *
   */
  class PeerInfo
  {
    String  peerId;
    String  orgId;
    String  ipAddress;
    boolean orgEventHub;
    String  endorsePort;
    String  eventingPort;
    String  tlsCertificate;
    boolean trustServerCert;
    String  sslProvider;
    String  negotiationType;
    boolean useTLS;
    boolean useMutualTLS;

    public String   getPeerId()          { return peerId;          }
    public String   getOrgId()           { return orgId;           }
    public String   getIpAddress()       { return ipAddress;       }
    public boolean  isOrgEventHub()      { return orgEventHub;     }
    public String   getEndorsePort()     { return endorsePort;     }
    public String   getEventingPort()    { return eventingPort;    }
    public String   getTlsCertificate()  { return tlsCertificate;  }
    public boolean  isTrustServerCert()  { return trustServerCert; }
    public String   getSslProvider()     { return sslProvider;     }
    public String   getNegotiationType() { return negotiationType; }
    public boolean  isUseTLS()           { return useTLS;          }
    public boolean  isUseMutualTLS()     { return useMutualTLS;    }

    public void setPeerId(          String   peerId          ) { this.peerId          = peerId;          }
    public void setOrgId(           String   orgId           ) { this.orgId           = orgId;           }
    public void setIpAddress(       String   ipAddress       ) { this.ipAddress       = ipAddress;       }
    public void setOrgEventHub(     boolean  orgEventHub     ) { this.orgEventHub     = orgEventHub;     }
    public void setEndorsePort(     String   endorsePort     ) { this.endorsePort     = endorsePort;     }
    public void setEventingPort(    String   eventingPort    ) { this.eventingPort    = eventingPort;    }
    public void setTlsCertificate(  String   tlsCertificate  ) { this.tlsCertificate  = tlsCertificate;  }
    public void setTrustServerCert( boolean  trustServerCert ) { this.trustServerCert = trustServerCert; }
    public void setSslProvider(     String   sslProvider     ) { this.sslProvider     = sslProvider;     }
    public void setNegotiationType( String   negotiationType ) { this.negotiationType = negotiationType; }
    public void setUseTLS(          boolean  useTLS          ) { this.useTLS = useTLS;                   }
    public void setUseMutualTLS(    boolean  useMutualTLS    ) { this.useMutualTLS = useMutualTLS;       }
  }
  
  /**
   * Temporary object for json parsing
   * @author tim
   *
   */
  class OrdererInfo
  {
    String  nodeId;
    String  orgId;
    String  ipAddress;
    String  ordererPort;
    String  tlsCertificate;
    boolean trustServerCert;
    String  sslProvider;
    String  negotiationType;

    public String   getNodeId()          { return nodeId;          }
    public String   getOrgId()           { return orgId;           }
    public String   getIpAddress()       { return ipAddress;       }
    public String   getOrdererPort()     { return ordererPort;     }
    public String   getTlsCertificate()  { return tlsCertificate;  }
    public boolean  isTrustServerCert()  { return trustServerCert; }
    public String   getSslProvider()     { return sslProvider;     }
    public String   getNegotiationType() { return negotiationType; }

    public void setNodeId(          String   nodeId          ) { this.nodeId          = nodeId;          }
    public void setOrgId(           String   orgId           ) { this.orgId           = orgId;           }
    public void setIpAddress(       String   ipAddress       ) { this.ipAddress       = ipAddress;       }
    public void setOrdererPort(     String   ordererPort     ) { this.ordererPort     = ordererPort;     }
    public void setTlsCertificate(  String   tlsCertificate  ) { this.tlsCertificate  = tlsCertificate;  }
    public void setTrustServerCert( boolean  trustServerCert ) { this.trustServerCert = trustServerCert; }
    public void setSslProvider(     String   sslProvider     ) { this.sslProvider     = sslProvider;     }
    public void setNegotiationType( String   negotiationType ) { this.negotiationType = negotiationType; }
  }
  
}
