module Main where

import Control.Concurrent (threadDelay)
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform.Async.AsyncChan hiding (worker)
import qualified Control.Distributed.Process.Platform.Async.AsyncChan as C (worker)
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
  runProcess node $ doWork True False
  runProcess node $ doWork False False
--  runProcess node $ doWork True False

--  runProcess node worker
--  runProcess node $ do
--    sleep $ seconds 10
--    say "done...."
  threadDelay $ (1*1000000)
  closeLocalNode node
  return ()

worker :: Process ()
worker = do
  server <- startGenServer
  _      <- monitor server

  mapM_ (\(n :: Int) -> (P.call server ("bar" ++ (show n))) :: Process String) [1..800]

  -- sleep $ seconds 3
  -- P.shutdown server
  -- receiveWait [ match (\(ProcessMonitorNotification _ _ _) -> return ()) ]

  say "server is idle now..."
  sleep $ seconds 5

doWork :: Bool -> Bool -> Process ()
doWork useAsync killServer =
  let call' = case useAsync of
               True  -> callAsync
               False -> call
  in do
    server <- spawnLocal $ forever' $ do
      receiveWait [ match (\(pid, _ :: String) -> send pid ()) ]

    mapM_ (\(n :: Int) -> (call' server (show n)) :: Process ()  ) [1..800]
    sleep $ seconds 4
    say "done"
    case killServer of
      True -> kill server "stop"
      False -> return ()
    sleep $ seconds 1

startLocalNode :: IO LocalNode
startLocalNode = do
  Right (transport,_) <- createTransportExposeInternals "127.0.0.1"
                                                        "8081"
                                                        defaultTCPParameters
  node <- newLocalNode transport initRemoteTable
  return node

callAsync :: ProcessId -> String -> Process ()
callAsync pid s = do
  asyncRef <- async $ AsyncTask $ call pid s
  (AsyncDone _) <- wait asyncRef
  return ()

call :: ProcessId -> String -> Process ()
call pid s = do
    self <- getSelfPid
    send pid (self, s)
    expect :: Process ()

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

