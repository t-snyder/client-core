package sdkwrapper.block.event;

import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.Peer;

import sdkwrapper.exceptions.BlockEventException;
import sdkwrapper.exceptions.InfrastructureException;
import sdkwrapper.runtimemgr.RuntimeMgrIF;

public interface BlockEventsPlayerIF
{
  public void initializeEventing( RuntimeMgrIF runtimeMgr, Channel channel, Peer peer )
   throws BlockEventException;
 
  public void shutdown()
   throws BlockEventException;
  
  public long getCurrentSeqNumber()
   throws BlockEventException;

}
