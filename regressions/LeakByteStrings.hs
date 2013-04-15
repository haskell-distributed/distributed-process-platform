module Main where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform.Async.AsyncChan hiding (worker)
import Control.Distributed.Process.Platform.ManagedProcess hiding
  (call, callAsync, shutdown, runProcess)
import qualified Control.Distributed.Process.Platform.ManagedProcess as P (call, shutdown)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Platform.Timer
import Control.Exception (SomeException)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import Prelude hiding (catch)

import Data.Time.Clock (getCurrentTime)
import Data.Time.Format (formatTime)
import System.Locale (defaultTimeLocale)

{-# INLINE forever' #-}
forever' :: Monad m => m a -> m b
forever' a = let a' = a >> a' in a'

main :: IO ()
main = do
  node <- startLocalNode
  runProcess node doWorkSimple
  closeLocalNode node
  return ()

worker :: Process ()
worker = do
  server <- startGenServer
  _      <- monitor server

  mapM_ (\(n :: Int) -> (P.call server ("bar" ++ (show n))) :: Process String) [1..800]
  -- mapM_ (\(n :: Int) -> (cast server ("bar" ++ (show n))) :: Process ()) [1..800]
  sleep $ seconds 1
  P.shutdown server

  receiveWait [ match (\(ProcessMonitorNotification _ _ _) -> return ()) ]
  say "server stopped..."
  sleep $ seconds 5

doNoWork :: Process ()
doNoWork = do
  mapM_ (\(n :: Int) -> newChan :: Process (SendPort (), ReceivePort ())) [1..800]
  -- mapM_ (\(n :: Int) -> (cast server ("bar" ++ (show n))) :: Process ()) [1..800]
  sleep $ seconds 3

doWorkSimple :: Process ()
doWorkSimple = do
  server <- spawnLocal $ forever' $ do
    receiveWait [ match (\(pid, _ :: String) -> send pid ()) ]

  mapM_ (\(n :: Int) -> (call server (show n)) :: Process ()  ) [1..800]
  sleep $ seconds 4
  say "done"
  sleep $ seconds 1

doWork :: Process ()
doWork = do
  server <- startServer
  _      <- monitor server

  mapM_ (\(n :: Int) -> (callAsync server) :: Process ()) [1..800]
  -- mapM_ (\(n :: Int) -> (cast server ("bar" ++ (show n))) :: Process ()) [1..800]
  sleep $ seconds 1
  shutdown server

  receiveWait [ match (\(ProcessMonitorNotification _ _ _) -> return ()) ]
  say "server stopped..."
  sleep $ seconds 5

startLocalNode :: IO LocalNode
startLocalNode = do
  Right (transport,_) <- createTransportExposeInternals "127.0.0.1"
                                                        "8081"
                                                        defaultTCPParameters
  node <- newLocalNode transport initRemoteTable
  return node

shutdown :: ProcessId -> Process ()
shutdown pid = send pid ()

callAsync :: ProcessId -> Process ()
callAsync pid = do
  asyncRef <- async $ AsyncTask $ do
    getSelfPid >>= send pid
    expect :: Process String
  (AsyncDone _) <- wait asyncRef
  return ()

call :: ProcessId -> String -> Process ()
call pid s = do
    self <- getSelfPid
    send pid (self, s)
    expect :: Process ()

startServer :: Process ProcessId
startServer = spawnLocal listen
  where listen = do
          receiveWait [
              match (\pid -> do
                        now <- liftIO getCurrentTime
                        let n = formatTime defaultTimeLocale "[%d-%m-%Y] [%H:%M:%S-%q]" now
                        send pid n)
            , match (\() -> die "terminating")
            ]
          listen

startGenServer :: Process ProcessId
startGenServer = do
  sid <- spawnLocal $  do
      catch (start () (statelessInit Infinity) serverDefinition >> return ())
            (\(e :: SomeException) -> say $ "failed with " ++ (show e))
  return sid

serverDefinition :: ProcessDefinition ()
serverDefinition =
  statelessProcess {
     apiHandlers = [
          handleCall_ (\(s :: String) -> return s)
        , handleCast  (\s (_ :: String) -> continue s)
        ]
  }


