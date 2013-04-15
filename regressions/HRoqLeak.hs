{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Applicative ((<$>), (<*>))
import Control.Concurrent
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node 
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Exception as Exception
import Data.List(elemIndices,isInfixOf)
import System.Directory
import System.IO
import System.IO.Error
import Control.Monad(when,replicateM,foldM,liftM3,liftM2,liftM)
import Control.Distributed.Process.Platform.Time
import Data.Binary
import Data.DeriveTH
import Data.Maybe
import Data.Typeable (Typeable)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.Map as Map
import qualified Data.ByteString.Lazy as L

-- ---------------------------------------------------------------------

main = do
  -- EKG.forkServer "localhost" 8000

  node <- startLocalNode

  -- runProcess node worker
  runProcess node worker

  closeLocalNode node
  
  return ()

-- ---------------------------------------------------------------------

worker :: Process ()
worker = do
  sid <- startHroqMnesia ()
  logm "mnesia started"
  
  let table = TN "mnesiattest"

  -- create_table DiscCopies table RecordTypeQueueEntry

  -- wait_for_tables [table] Infinity

  -- ms2 <- get_state
  -- logm $ "mnesia state ms2:" ++ (show ms2)

  let qe = QE (QK "a") (qval $ "bar2")
  mapM_ (\n -> dirty_write_q sid table (QE (QK "a") (qval $ "bar" ++ (show n)))) [1..800]
  -- mapM_ (\n -> dirty_write_q table qe) [1..800]
  -- mapM_ (\n -> do_dirty_write_q s table qe) [1..800]

  liftIO $ threadDelay (5*1000000) -- 1 seconds

  say $ "mnesia blurble"

  liftIO $ threadDelay (2*60*1000000)
  -- liftIO $ threadDelay (10*60*1000000) -- Ten minutes
  return ()

-- ---------------------------------------------------------------------
-- ---------------------------------------------------------------------  

startLocalNode :: IO LocalNode
startLocalNode = do
    -- [role, host, port] <- getArgs
  let [role, host, port] = ["foo","127.0.0.1", "10519"]
  -- Right transport <- createTransport host port defaultTCPParameters
  Right (transport,_internals) <- createTransportExposeInternals host port defaultTCPParameters
  node <- newLocalNode transport initRemoteTable
  -- startLoggerProcess node
  return node

logm = say

-- ---------------------------------------------------------------------

-- qval str = QV $ Map.fromList [(str,str)]
qval str = QV str

-- ---------------------------------------------------------------------

data QKey = QK !String
            deriving (Typeable,Show,Read,Eq,Ord)

instance Binary QKey where
  put (QK i) = put i
  get = do
    i <- get
    return $ QK i


-- data QValue = QV !(Map.Map String String)
data QValue = QV !String
              deriving (Typeable,Read,Show)
data QEntry = QE !QKey    -- ^Id
                 !QValue  -- ^payload
              deriving (Typeable,Read,Show)

instance Binary QValue where
  put (QV v) = put v
  get = QV <$> get


instance Binary QEntry where
  put (QE k v) = put k >> put v
  get = do
    k <- get
    v <- get
    return $ QE k v 

-- ---------------------------------------------------------------------

startHroqMnesia :: a -> Process ProcessId
startHroqMnesia initParams = do
  let server = serverDefinition
  sid <- spawnLocal $ start initParams initFunc server >> return ()
  -- register hroqMnesiaName sid
  return sid

data State = ST Int

-- init callback
initFunc :: InitHandler a State
initFunc _ = do
  let s = ST 0
  return $ InitOk s Infinity

--  , dirty_write_q
data DirtyWriteQ = DirtyWriteQ !TableName !QEntry
                   deriving (Typeable, Show)

instance Binary DirtyWriteQ where
  put (DirtyWriteQ tn key) = put tn >> put key
  get = DirtyWriteQ <$> get <*> get


-- , get_state
data GetState = GetState
                deriving (Typeable,Show)

instance Binary GetState where
  put GetState = putWord8 1
  get = do
          v <- getWord8
          case v of
            1 -> return GetState

-- ---------------------------------------------------------------------

data TableName = TN !String
                 deriving (Show,Read,Typeable,Eq,Ord)

instance Binary TableName where
  put (TN s) = put s
  get = do
    s <- get
    return (TN s)

-- -----------------------------------------------------------------------------
-- API

dirty_write_q :: ProcessId -> TableName -> QEntry -> Process ()
dirty_write_q sid tablename val = call sid (DirtyWriteQ tablename val)

--------------------------------------------------------------------------------
-- Implementation                                                             --
--------------------------------------------------------------------------------

serverDefinition :: ProcessDefinition State
serverDefinition = defaultProcess {
      apiHandlers = [
            handleCall handleDirtyWriteQ
          ]
    , infoHandlers =
        [
        -- handleInfo_ (\(ProcessMonitorNotification _ _ r) -> logm $ show r >> continue_)
           handleInfo (\dict (ProcessMonitorNotification _ _ r) -> do {logm $ show r; continue dict })
        ]
    , timeoutHandler = \_ _ -> stop $ TerminateOther "timeout az"
    , terminateHandler = \_ reason -> do { logm $ "HroqMnesia terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State

-- ---------------------------------------------------------------------
-- handlers

handleDirtyWriteQ :: State -> DirtyWriteQ -> Process (ProcessReply State ())
handleDirtyWriteQ s (DirtyWriteQ tableName val) = do
    s' <- do_dirty_write_q s tableName val
    reply () s'

-- ---------------------------------------------------------------------
-- actual workers

do_dirty_write_q ::
   State -> TableName -> QEntry -> Process State
do_dirty_write_q s tableName record = do
  -- logm $ "dirty_write:" ++ (show (tableName,record))
  liftIO $ defaultAppend (tableNameToFileName tableName) (encode record)

  -- let s' = insertEntryQ s tableName record
  -- return s'
  return s

-- ---------------------------------------------------------------------

directoryPrefix :: String
directoryPrefix = ".hroqdata/"

tableNameToFileName :: TableName -> FilePath
tableNameToFileName (TN tableName) = directoryPrefix ++ tableName


-- ---------------------------------------------------------------------

defaultWrite  :: FilePath -> B.ByteString -> IO ()
defaultWrite  filename x = safeFileOp B.writeFile  filename x

defaultAppend :: FilePath -> B.ByteString -> IO ()
defaultAppend filename x = safeFileOp B.appendFile filename x

-- ---------------------------------------------------------------------

safeFileOp :: (FilePath -> B.ByteString -> IO ()) -> FilePath -> B.ByteString -> IO ()
safeFileOp op filename str= handle  handler  $ op filename str  -- !> ("write "++filename)
     where
     handler e-- (e :: IOError)
       | isDoesNotExistError e=do
                  createDirectoryIfMissing True $ take (1+(last $ elemIndices '/' filename)) filename   --maybe the path does not exist
                  safeFileOp op filename str

       | otherwise= if ("invalid" `isInfixOf` ioeGetErrorString e)
             then
                error  $ "writeResource: " ++ show e ++ " defPath and/or keyResource are not suitable for a file path"
             else do
                hPutStrLn stderr $ "defaultWriteResource:  " ++ show e ++  " in file: " ++ filename ++ " retrying"
                safeFileOp op filename str


