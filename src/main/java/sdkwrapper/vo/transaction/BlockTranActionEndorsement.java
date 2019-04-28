package sdkwrapper.vo.transaction;

/**
 * Represents an Endorsement of a particular transaction or Transaction Action if the transaction sent to the
 * orderer is an aggregate transaction.
 */
public class BlockTranActionEndorsement
{
  private String endorserCert = null;
  private String signature    = null;
  
  public String getEndorserCert() { return endorserCert; }
  public String getSignature()    { return signature;    }
  
  public void setEndorserCert( String endorserCert ) { this.endorserCert = endorserCert; }
  public void setSignature(    String signature    ) { this.signature    = signature;    }
}
