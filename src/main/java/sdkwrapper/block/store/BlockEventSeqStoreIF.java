package sdkwrapper.block.store;

import sdkwrapper.exceptions.BlockEventException;

public interface BlockEventSeqStoreIF
{
  public long getCurrentSequenceNumber( String channelId )
    throws BlockEventException;
  
  public void putBlockSeqNumber( String channelId, long seqNumber )
    throws BlockEventException;
}
