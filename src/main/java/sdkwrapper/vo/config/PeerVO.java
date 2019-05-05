package sdkwrapper.vo.config;

import java.io.Serializable;

import org.hyperledger.fabric.sdk.EventHub;
import org.hyperledger.fabric.sdk.Peer;

import sdkwrapper.exceptions.ConfigurationException;

public class PeerVO implements Serializable
{
  private static final long serialVersionUID = -2236035032905116485L;

  private String  peerId          = null;
  private String  orgId           = null;
  private String  ipAddress       = null;
  private boolean orgEventHub     = false;
  private String  endorsePort     = null;
  private String  eventingPort    = null;
  private boolean useTLS          = false;
  private boolean useMutualTLS    = false;
  private String  tlsCertificate  = null;
  private boolean trustServerCert = false;
  private String  sslProvider     = "openSSL";
  private String  negotiationType = "TLS";
 
  // Transient attributes
  private Peer     sdkPeer        = null;
  private EventHub sdkEventHub    = null;
  
  public String   getPeerId()          { return peerId;          }
  public String   getOrgId()           { return orgId;           }
  public String   getIpAddress()       { return ipAddress;       }
  public boolean  isOrgEventHub()      { return orgEventHub;     }
  public String   getEndorsePort()     { return endorsePort;     }
  public String   getEventingPort()    { return eventingPort;    }
  public boolean  getUseTLS()          { return useTLS;          }
  public boolean  getUseMutualTLS()    { return useMutualTLS;    }
  public String   getTlsCertificate()  { return tlsCertificate;  }
  public boolean  isTrustServerCert()  { return trustServerCert; }
  public String   getSslProvider()     { return sslProvider;     }
  public String   getNegotiationType() { return negotiationType; }
  public Peer     getSdkPeer()         { return sdkPeer;         }
  public EventHub getSdkEventHub()     { return sdkEventHub;     }
  
  public void setPeerId(          String   peerId          ) { this.peerId          = peerId;          }
  public void setOrgId(           String   orgId           ) { this.orgId           = orgId;           }
  public void setIpAddress(       String   ipAddress       ) { this.ipAddress       = ipAddress;       }
  public void setOrgEventHub(     boolean  orgEventHub     ) { this.orgEventHub     = orgEventHub;     }
  public void setEndorsePort(     String   endorsePort     ) { this.endorsePort     = endorsePort;     }
  public void setEventingPort(    String   eventingPort    ) { this.eventingPort    = eventingPort;    }
  public void setUseTLS(          boolean  useTLS          ) { this.useTLS          = useTLS;          }
  public void setUseMutualTLS(    boolean  useMutualTLS    ) { this.useMutualTLS    = useMutualTLS;    }
  public void setTlsCertificate(  String   tlsCertificate  ) { this.tlsCertificate  = tlsCertificate;  }
  public void setTrustServerCert( boolean  trustServerCert ) { this.trustServerCert = trustServerCert; }
  public void setSslProvider(     String   sslProvider     ) { this.sslProvider     = sslProvider;     }
  public void setNegotiationType( String   negotiationType ) { this.negotiationType = negotiationType; }
  public void setSdkPeer(         Peer     peer            ) { this.sdkPeer         = peer;            }
  public void setSdkEventHub(     EventHub eventHub        ) { this.sdkEventHub     = eventHub;        }
  
  /**
   * Returns the configured Endorser URL in the format (without the brackets):
   *   <peerId>@grpc://<ipAddress>:<endorsePort>
   * @return
   * @throws ConfigurationException
   */
  public String getEndorseUrl()
   throws ConfigurationException
  {
    if( peerId      == null ) throw new ConfigurationException( "Peer Id is not set"      );
    if( ipAddress   == null ) throw new ConfigurationException( "IP Address is not set"   );
    if( endorsePort == null ) throw new ConfigurationException( "Endorse Port is not set" );

    String protocol = this.getUseTLS() ? "grpcs://" : "grpc://";

//System.out.println( peerId + "@grpc://" + ipAddress + ":" + endorsePort );
//    return peerId + "@grpc://" + ipAddress + ":" + endorsePort;
    System.out.println( "Endorsement URL for " + peerId + " = " + protocol + ipAddress + ":" + endorsePort );
    return protocol + ipAddress + ":" + endorsePort;
  }
  
  /**
   * Returns the configured Eventing URL in the format (without the brackets):
   *   <peerId>@grpc://<ipAddress>:<eventingPort>
   * @return
   * @throws ConfigurationException
   */
  public String getEventingUrl()
   throws ConfigurationException
  {
    if( peerId       == null ) throw new ConfigurationException( "Peer Id is not set"      );
    if( ipAddress    == null ) throw new ConfigurationException( "IP Address is not set"   );
    if( eventingPort == null ) throw new ConfigurationException( "Eventing Port is not set" );

    String protocol = this.getUseTLS() ? "grpcs://" : "grpc://";

    System.out.println( "Event Hub URL for " + peerId + " = " + protocol + ipAddress + ":" + eventingPort );
    return protocol + ipAddress + ":" + eventingPort;
  }

}
