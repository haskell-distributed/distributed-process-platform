module Main where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform.Async.AsyncChan
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Platform.Timer
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import Prelude hiding (catch)

import Data.Time.Clock (getCurrentTime)
import Data.Time.Format (formatTime)
import System.Locale (defaultTimeLocale)

main :: IO ()
main = do
  node <- startLocalNode
  runProcess node doWork
  closeLocalNode node
  return ()

doWork :: Process ()
doWork = do
  server <- startServer
  _      <- monitor server

  mapM_ (\(n :: Int) -> (call server) :: Process ()) [1..800]
  -- mapM_ (\(n :: Int) -> (cast server ("bar" ++ (show n))) :: Process ()) [1..800]
  sleep $ seconds 1
  shutdown server

  receiveWait [ match (\(ProcessMonitorNotification _ _ _) -> return ()) ]
  say "server stopped..."
  sleep $ seconds 2

startLocalNode :: IO LocalNode
startLocalNode = do
  Right (transport,_) <- createTransportExposeInternals "127.0.0.1"
                                                        "8081"
                                                        defaultTCPParameters
  node <- newLocalNode transport initRemoteTable
  return node

shutdown :: ProcessId -> Process ()
shutdown pid = send pid ()

call :: ProcessId -> Process ()
call pid = do
    (sp, rp) <- newChan :: Process (SendPort String, ReceivePort String)
    send pid sp
    n <- receiveChan rp
    return ()

startServer :: Process ProcessId
startServer = spawnLocal listen
  where listen = do
          receiveWait [
              match (\sp -> do
                        now <- liftIO getCurrentTime
                        us  <- getSelfPid
                        let n = formatTime defaultTimeLocale "[%d-%m-%Y] [%H:%M:%S-%q]" now
                        sendChan sp n)
            , match (\() -> die "terminating")
            ]
          listen

