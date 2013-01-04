{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE TemplateHaskell           #-}

module Main where

import Prelude hiding (catch)
import Control.Monad (forever)
import Control.Concurrent.MVar
  ( newEmptyMVar
  , putMVar
  , takeMVar
  , withMVar
  )
-- import Control.Applicative ((<$>), (<*>), pure, (<|>))
import qualified Network.Transport as NT (Transport)
import Network.Transport.TCP()
import Control.Distributed.Platform
import Control.Distributed.Process
import Control.Distributed.Process.Node
import Control.Distributed.Process.Serializable()
import Control.Distributed.Platform.Timer

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)

import TestUtils

testSendAfter :: TestResult Bool -> Process ()
testSendAfter result =  do
  let delay = seconds 1
  pid <- getSelfPid
  _ <- sendAfter delay pid Ping
  hdInbox <- receiveTimeout (intervalToMs delay * 4) [
                                 match (\m@(Ping) -> return m) ]
  case hdInbox of
      Just Ping -> stash result True
      Nothing   -> stash result False

testRunAfter :: TestResult Bool -> Process ()
testRunAfter result = do
  let delay = seconds 2

  parentPid <- getSelfPid
  _ <- spawnLocal $ do
    _ <- runAfter delay $ send parentPid Ping
    return ()

  msg <- expectTimeout (intervalToMs delay * 4)
  case msg of
      Just Ping -> stash result True
      Nothing   -> stash result False
  return ()

testCancelTimer :: TestResult Bool -> Process ()
testCancelTimer result = do
  let delay = milliseconds 50
  pid <- periodically delay noop
  ref <- monitor pid

  sleep $ seconds 1
  cancelTimer pid

  _ <- receiveWait [
        match (\(ProcessMonitorNotification ref' pid' _) ->
                stash result $ ref == ref' && pid == pid') ]

  return ()

testPeriodicSend :: TestResult Bool -> Process ()
testPeriodicSend result = do
  let delay = milliseconds 100
  self <- getSelfPid
  ref <- ticker delay self
  listener 0 ref
  liftIO $ putMVar result True
  where listener :: Int -> TimerRef -> Process ()
        listener n tRef | n > 10    = cancelTimer tRef
                        | otherwise = waitOne >> listener (n + 1) tRef
        -- get a single tick, blocking indefinitely
        waitOne :: Process ()
        waitOne = do
            Tick <- expect
            return ()

testTimerReset :: TestResult Int -> Process ()
testTimerReset result = do
  let delay = seconds 10
  counter <- liftIO $ newEmptyMVar

  listenerPid <- spawnLocal $ do
      stash counter 0
      -- we continually listen for 'ticks' and increment counter for each
      forever $ do
        Tick <- expect
        liftIO $ withMVar counter (\n -> (return (n + 1)))

  -- this ticker will 'fire' every 10 seconds
  ref <- ticker delay listenerPid

  sleep $ seconds 2
  resetTimer ref

  -- at this point, the timer should be back to roughly a 5 second count down
  -- so our few remaining cycles no ticks ought to make it to the listener
  -- therefore we kill off the timer and the listener now and take the count
  cancelTimer ref
  kill listenerPid "stop!"

  -- how many 'ticks' did the listener observer? (hopefully none!)
  count <- liftIO $ takeMVar counter
  liftIO $ putMVar result count

testLinkingWithNormalExits :: TestResult DiedReason -> Process ()
testLinkingWithNormalExits result = do
  testPid <- getSelfPid
  pid <- spawnLocal $ do
    worker <- spawnLocal $ do
      "finish" <- expect
      return ()
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
      ]
  ]

primitivesTests :: NT.Transport -> IO [Test]
primitivesTests transport = do
  localNode <- newLocalNode transport initRemoteTable
  let testData = tests localNode
  return testData

main :: IO ()
main = testMain $ primitivesTests
