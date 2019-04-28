package sdkwrapper.block.event;

import static org.hyperledger.fabric.sdk.Channel.PeerOptions.createPeerOptions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.hyperledger.fabric.sdk.BlockEvent;
import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.Channel.PeerOptions;
import org.hyperledger.fabric.sdk.Peer.PeerRole;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.Peer;

import sdkwrapper.block.listener.FabricBlockListener;
import sdkwrapper.block.store.BlockEventSeqFileStore;
import sdkwrapper.block.store.BlockEventSeqStoreIF;
import sdkwrapper.exceptions.BlockEventException;
import sdkwrapper.exceptions.InfrastructureException;
import sdkwrapper.runtimemgr.RuntimeMgr;
import sdkwrapper.runtimemgr.RuntimeMgrIF;

/**
 * Store the last block processed sequence number in a file.
 *  
 * @author tim
 *
 */
public class BlockEventFileStorePlayer implements BlockEventsPlayerIF
{
  private static final Logger logger = LogManager.getLogger( BlockEventFileStorePlayer.class );

  private Channel              channel    = null;
  private FabricBlockListener  listener   = null;
  private BlockEventSeqStoreIF seqStore   = null; 
  private String               listenerId = null;
  
  @Override
  public long getCurrentSeqNumber()
    throws BlockEventException
  {
    return seqStore.getCurrentSequenceNumber( channel.getName() );
  }

  @Override
  public void initializeEventing( RuntimeMgrIF runtimeMgr, Channel channel, Peer peer ) 
    throws BlockEventException
  {
    logger.info( "Starting playEvents" );

    if( channel == null )
      throw new BlockEventException( "Channel is null." );
    
    if( peer == null )
      throw new BlockEventException( "Peer is null." );
 
    this.channel  = channel;
    this.seqStore = runtimeMgr.getBlockSeqStore();

    if( !channel.getPeers().contains( peer ))
    {
      // the channel does not contain the Peer. This should never happen, but in the interests of defensive 
      // programming we add the peer to the channel
      final PeerOptions peerOptions = createPeerOptions().setPeerRoles(EnumSet.of( PeerRole.SERVICE_DISCOVERY, PeerRole.LEDGER_QUERY, PeerRole.EVENT_SOURCE, PeerRole.CHAINCODE_QUERY, PeerRole.ENDORSING_PEER ));
      final long        startSeq    = seqStore.getCurrentSequenceNumber( channel.getName() ) + 1;
      try
      {
        channel.addPeer( peer, peerOptions.startEvents( startSeq ));
        logger.info( "Added peer " + peer.getName() + " to channel = " + channel.getName() );
      } 
      catch( InvalidArgumentException e )
      {
        final String msg = "Error adding Event Source Peer to Channel. Error = " + e.getMessage();
        logger.error( msg );
        throw new BlockEventException( msg );
      }
    }
    
    this.listener = new FabricBlockListener( channel, runtimeMgr.getBlockEventProcessor() );
    
    try
    {
      listenerId = channel.registerBlockListener( listener );
      logger.info( "Successfully registered block listener on channel = " + channel.getName() );
    } 
    catch( InvalidArgumentException e )
    {
      String msg = "Error registering Block Event Listener with Channel. Error = " + e.getMessage();
      logger.error( msg );
      throw new BlockEventException( msg );
    }
  }

  @Override
  public void shutdown() 
    throws BlockEventException
  {
    try
    {
      channel.unregisterBlockListener( listenerId );
    } 
    catch( InvalidArgumentException e )
    {
      String msg = "Error unregistering Block Event Listener with Channel. Error = " + e.getMessage();
      logger.error( msg );
      throw new BlockEventException( msg );
    }
  }


}
