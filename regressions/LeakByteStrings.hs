module Main where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Platform.Timer
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

shutdown :: ProcessId -> Process ()
shutdown pid = send pid ()

call :: ProcessId -> String -> Process ()
call pid msg = do
    asyncRef <- async $ do
      mRef <- monitor pid
      self <- getSelfPid
      send pid (self, msg)
      r <- receiveWait [
            match (\() -> return Nothing)
          , matchIf
                (\(ProcessMonitorNotification ref _ _)    -> ref == mRef)
                (\(ProcessMonitorNotification _ _ reason) -> return (Just reason))
          ]
      unmonitor mRef
      case r of
        Nothing  -> return ()
        Just err -> die $ "ServerExit (" ++ (show err) ++ ")"
    asyncResult <- wait asyncRef
    case asyncResult of
      (AsyncDone ()) -> return ()
      _              -> die "unexpected async result"

startServer :: Process ProcessId
startServer = spawnLocal listen
  where listen = do
          receiveWait [
              match (\(pid, _ :: String) -> say "got string" >> send pid ())
            , match (\() -> die "terminating")
            ]
          listen

