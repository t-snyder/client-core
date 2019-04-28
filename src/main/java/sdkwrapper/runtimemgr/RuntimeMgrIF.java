package sdkwrapper.runtimemgr;

import sdkwrapper.block.store.BlockEventSeqStoreIF;
import sdkwrapper.config.ConfigProperties;
import sdkwrapper.error.ErrorCommandIF;
import sdkwrapper.error.ErrorController;
import sdkwrapper.events.BlockEventProcessorIF;
import sdkwrapper.exceptions.ConfigurationException;
import sdkwrapper.exceptions.InfrastructureException;
import sdkwrapper.service.FabricServices;

public interface RuntimeMgrIF
{
  public ErrorController       getErrorController();
  public FabricServices        getFabricServices();
  public BlockEventSeqStoreIF  getBlockSeqStore();
  public BlockEventProcessorIF getBlockEventProcessor();
  public ConfigProperties      getConfig();
  
  public void initialize( String appPropsPath, String sdkConfigPath ) 
    throws ConfigurationException, InfrastructureException;

  public void runErrorCmd( ErrorCommandIF cmd );
}
