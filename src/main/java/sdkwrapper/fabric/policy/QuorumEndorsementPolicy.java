package sdkwrapper.fabric.policy;

import java.util.Collection;
import java.util.HashSet;

import org.hyperledger.fabric.sdk.ProposalResponse;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;

/**
 * This implements a simplistic Quorum (majority) Endorsement Policy for the responses received.
 * A majority of the endorsement responses must be successful. 
 * 
 * This is not suitable for production as it does not take into account multiple Peer endorsements from the same org.
 * 
 * @author tim
 *
 */
public class QuorumEndorsementPolicy implements EndorsementPolicyIF
{
  private Collection<ProposalResponse> failedResponses  = new HashSet<ProposalResponse>();
  private Collection<ProposalResponse> successResponses = new HashSet<ProposalResponse>();
  
  
  public boolean isPolicyMet(Collection<ProposalResponse> responses)
  {
    failedResponses.clear();
    successResponses.clear();
    
    int totalResponses = responses.size();
    int quorum         = totalResponses/2 + 1;
    
    try
    {
      for( ProposalResponse response : responses )
      {
        int returnCode = response.getChaincodeActionResponseStatus();
        if( returnCode >= 200 && returnCode < 400 )
        {
          successResponses.add( response );
        }
        else if( returnCode >= 400 )
        {
          failedResponses.add( response );
        }
      }
    }
    catch( InvalidArgumentException e )
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    if( successResponses.size() >= quorum )
      return true;
    
    return false;
  }

  public Collection<ProposalResponse> getFailedResponses()
  {
    return failedResponses;
  }

  public Collection<ProposalResponse> getSuccessfulResponses()
  {
    return successResponses;
  }

}
