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

{-# INLINE forever' #-}
forever' :: Monad m => m a -> m b
forever' a = let a' = a >> a' in a'

main :: IO ()
main = do
  node <- startLocalNode
  runProcess node doWork
  closeLocalNode node
  return ()

doNoWork :: Process ()
doNoWork = do
  mapM_ (\(n :: Int) -> newChan :: Process (SendPort (), ReceivePort ())) [1..800]
  -- mapM_ (\(n :: Int) -> (cast server ("bar" ++ (show n))) :: Process ()) [1..800]
  sleep $ seconds 3

doWorkSimple :: Process ()
doWorkSimple = do
  server <- spawnLocal $ forever' $ do
    receiveWait [ match (\(s :: String) -> do
                            now <- liftIO getCurrentTime
                            us  <- getSelfPid
                            let n = formatTime defaultTimeLocale
                                               "[%d-%m-%Y] [%H:%M:%S-%q]"
                                               now
                            return (show n) >> return ()) ]

  mapM_ (\(n :: Int) -> (send server ("bar" ++ (show n))) :: Process ()  ) [1..800]
  sleep $ seconds 2

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

call :: ProcessId -> Process ()
call pid = do
    self <- getSelfPid
    send pid self
    n <- expect :: Process String
    return ()

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

