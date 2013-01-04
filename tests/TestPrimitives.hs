{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE TemplateHaskell           #-}

module Main where

import Prelude hiding (catch)
-- import Control.Applicative ((<$>), (<*>), pure, (<|>))
import qualified Network.Transport as NT (Transport)
import Network.Transport.TCP()
import Control.Distributed.Platform
import Control.Distributed.Process
import Control.Distributed.Process.Node
import Control.Distributed.Process.Serializable()

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)

import TestUtils

testLinkingWithNormalExits :: TestResult DiedReason -> Process ()
testLinkingWithNormalExits result = do
  testPid <- getSelfPid
  pid <- spawnLocal $ do
    worker <- spawnLocal $ do
      "finish" <- expect
      return ()
    linkOnFailure worker
    send testPid worker
    () <- expect
    return ()

  workerPid <- expect :: Process ProcessId
  ref <- monitor workerPid

  send workerPid "finish"
  receiveWait [
      matchIf (\(ProcessMonitorNotification ref' _ _) -> ref == ref')
              (\_ -> return ())
    ]

  -- by now, the worker is gone, so we can check that the
  -- insulator is still alive and well and that it exits normally
  -- when asked to do so
  ref2 <- monitor pid
  send pid ()

  r <- receiveWait [
      matchIf (\(ProcessMonitorNotification ref2' _ _) -> ref2 == ref2')
              (\(ProcessMonitorNotification _ _ reason) -> return reason)
    ]
  stash result r

testLinkingWithAbnormalExits :: TestResult (Maybe Bool) -> Process ()
testLinkingWithAbnormalExits result = do
  testPid <- getSelfPid
  pid <- spawnLocal $ do
    worker <- spawnLocal $ do
      "finish" <- expect
      return ()

    linkOnFailure worker
    send testPid worker
    () <- expect
    return ()

  workerPid <- expect :: Process ProcessId

  ref <- monitor pid
  kill workerPid "finish"  -- note the use of 'kill' instead of send
  r <- receiveTimeout (intervalToMs $ seconds 20) [
      matchIf (\(ProcessMonitorNotification ref' _ _) -> ref == ref')
              (\(ProcessMonitorNotification _ _ reason) -> return reason)
    ]
  case r of
    Just (DiedException _) -> stash result $ Just True
    (Just _)               -> stash result $ Just False
    Nothing                -> stash result Nothing

--------------------------------------------------------------------------------
-- Utilities and Plumbing                                                     --
--------------------------------------------------------------------------------

tests :: LocalNode  -> [Test]
tests localNode = [
    testGroup "Linking Tests" [
        testCase "testLinkingWithNormalExits"
                 (delayedAssertion
                  "normal exit should not terminate the caller"
                  localNode DiedNormal testLinkingWithNormalExits)
      , testCase "testLinkingWithAbnormalExits"
                 (delayedAssertion
                  "abnormal exit should terminate the caller"
                  localNode (Just True) testLinkingWithAbnormalExits)
      ]
  ]

primitivesTests :: NT.Transport -> IO [Test]
primitivesTests transport = do
  localNode <- newLocalNode transport initRemoteTable
  let testData = tests localNode
  return testData

main :: IO ()
main = testMain $ primitivesTests

