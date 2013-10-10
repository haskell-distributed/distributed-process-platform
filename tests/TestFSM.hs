{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE TemplateHaskell     #-}

module Main where

import Control.Concurrent.MVar
import Control.Exception (SomeException)
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform hiding (__remoteTable, monitor, send, nsend)
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess
import qualified Control.Distributed.Process.Platform.ManagedProcess.FSM as FSM (serve)
import Control.Distributed.Process.Platform.ManagedProcess.FSM
  ( (|>)
  , await
  , complete
  )
import Control.Distributed.Process.Platform.Test
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Platform.Timer
import Control.Distributed.Process.Serializable()

#if ! MIN_VERSION_base(4,6,0)
import Prelude hiding (catch)
#endif

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import TestUtils
-- import ManagedProcessCommon

import qualified Network.Transport as NT
import Control.Monad (void)

-- utilities

testRunningBasicFSM :: TestResult (Maybe DiedReason) -> Process ()
testRunningBasicFSM result = do
    pid <- spawnLocal $ FSM.serve () $ await |> complete
    monitor pid
    send pid "hi there!"
    awaitCompletion >>= stash result
  where
    awaitCompletion = do
      receiveTimeout (after 2 Seconds)
                     [ match (\(ProcessMonitorNotification _ _ r) -> return r) ]

-- myRemoteTable :: RemoteTable
-- myRemoteTable = Main.__remoteTable initRemoteTable

tests :: NT.Transport  -> IO [Test]
tests transport = do
  localNode <- newLocalNode transport initRemoteTable
  return [
    testGroup "basic server functionality"
    [
      testCase "Running a basic FSM"
        (delayedAssertion
         "expected the server to return the task outcome"
         localNode (Just DiedNormal) testRunningBasicFSM)
    ]
    ]

main :: IO ()
main = testMain $ tests


