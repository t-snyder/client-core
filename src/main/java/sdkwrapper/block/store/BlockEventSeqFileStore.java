package sdkwrapper.block.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import sdkwrapper.exceptions.BlockEventException;
import sdkwrapper.service.FabricServices;

public class BlockEventSeqFileStore implements BlockEventSeqStoreIF
{
  private final  Logger logger = LogManager.getLogger( BlockEventSeqFileStore.class );
  
  // filepath within the client app kube yaml file
  private static final String         filePath     = "/usr/src/app/block-seq/block-pos.ser";
  private static BlockEventSeqStoreIF instance     = null;
  private static Map<String, Long>    persistedMap = null;
  
  
  private BlockEventSeqFileStore()
  {
    // Do nothing
  }
  
  public static BlockEventSeqStoreIF getInstance()
    throws BlockEventException
  {
    if( instance == null )
    {
      BlockEventSeqFileStore fileInstance = new BlockEventSeqFileStore();
      
      persistedMap = fileInstance.readFile();

      if( persistedMap == null )
        throw new BlockEventException( "System error reading block event positions." );

      instance = fileInstance;
    }
    
    return instance;
  }
  
  @Override
  public long getCurrentSequenceNumber( String channelId ) 
    throws BlockEventException
  {
    if( channelId == null )
      throw new BlockEventException( "Null channelId" );
  
    if( persistedMap.containsKey( channelId ))
    {
      return persistedMap.get( channelId );
    }
    else
    {
      putBlockSeqNumber( channelId, 0L );
      return 0L;
    }
  }

  @Override
  public void putBlockSeqNumber( String channelId, long seqNumber ) 
    throws BlockEventException
  {
    boolean updated = false;

    if( channelId == null )
      throw new BlockEventException( "null channelId in BlockEventSequenceFileStore.putBlockSeqNumber" );
    
    if( seqNumber < 0 )
      throw new BlockEventException( "invalid seqNumber in BlockEventSequenceFileStore.putBlockSeqNumber" );
    
    if( persistedMap.containsKey( channelId ))
    {
      long seq = persistedMap.get( channelId );
      if( seqNumber > seq )
      {
        persistedMap.put( channelId, seqNumber );
        updated = true;
      }
    }
    else
    {
      persistedMap.put( channelId, seqNumber );
      updated = true;
    }
    
    if( updated )
    {
      writeFile();
      logger.info( "Channel block seq number processed updated to " + seqNumber );
    }
  }
  
  @SuppressWarnings( "unchecked" )
  private Map<String, Long> readFile()
    throws BlockEventException
  {
    ConcurrentHashMap<String, Long> map = null;

    try
    {
      File f = new File( filePath );
      if( f.exists() && !f.isDirectory() ) 
      { 
        FileInputStream   fis = new FileInputStream( filePath );
        ObjectInputStream ois = new ObjectInputStream(fis);
       
        map = (ConcurrentHashMap<String, Long>) ois.readObject();

        ois.close();
        fis.close();
      }
      else
      {
        map = new ConcurrentHashMap<String, Long>();
      }
      
      return map;
    }
    catch(IOException e )
    {
      final String msg = "Error reading BlockSequenceFileStore. Error = " + e.getMessage();
      logger.error( msg );
      throw new BlockEventException( msg );
    }
    catch( ClassNotFoundException e )
    {
      final String msg = "Error reading BlockSequenceFileStore. Error = " + e.getMessage();
      logger.error( msg );
      throw new BlockEventException( msg );
    }
  }

  private void writeFile()
    throws BlockEventException
  {
    FileOutputStream   fileOut = null;
    ObjectOutputStream objOut  = null;
    
    try
    {
      fileOut = new FileOutputStream( filePath );
      objOut  = new ObjectOutputStream( fileOut );
      
      objOut.writeObject( persistedMap );
      objOut.close();
    } 
    catch( IOException e )
    {
      final String msg = "Error writing BlockSequenceFileStore. Error = " + e.getMessage();
      logger.error( msg );
      throw new BlockEventException( msg );
    }
    
  }
}
