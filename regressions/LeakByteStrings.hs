module Main where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Platform.Timer
import Control.Exception (SomeException)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import Prelude hiding (catch)

main :: IO ()
main = do
  node <- startLocalNode
  runProcess node worker
  closeLocalNode node
  return ()

worker :: Process ()
worker = do
  server <- startServer
  _      <- monitor server

  mapM_ (\(n :: Int) -> (call server ("bar" ++ (show n))) :: Process ()) [1..800]
  -- mapM_ (\(n :: Int) -> (cast server ("bar" ++ (show n))) :: Process ()) [1..800]
  sleep $ seconds 1
  shutdown server

  receiveWait [ match (\(ProcessMonitorNotification _ _ _) -> return ()) ]
  say "server stopped..."
  sleep $ seconds 1

startLocalNode :: IO LocalNode
startLocalNode = do
  Right (transport,_) <- createTransportExposeInternals "127.0.0.1"
                                                        "8081"
                                                        defaultTCPParameters
  node <- newLocalNode transport initRemoteTable
  return node

startServer :: Process ProcessId
startServer = do
  sid <- spawnLocal $  do
      catch (start () (statelessInit Infinity) serverDefinition >> return ())
            (\(e :: SomeException) -> say $ "failed with " ++ (show e))
  return sid

serverDefinition :: ProcessDefinition ()
serverDefinition =
  statelessProcess {
     apiHandlers = [
          handleCall_ (\(_ :: String) -> say "called received string.." >> return ())
        , handleCast  (\s (_ :: String) -> say "cast received string.." >> continue s)
        ]
  }

