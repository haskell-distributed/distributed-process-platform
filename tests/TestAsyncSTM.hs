{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module TestAsyncSTM where

import Control.Concurrent.MVar
import Control.Distributed.Process
import Control.Distributed.Process.Node
import Control.Distributed.Process.Serializable()
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.Async.AsyncSTM
import Control.Distributed.Process.Platform.Test
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Platform.Timer

import Data.Binary()
import Data.Typeable()
import qualified Network.Transport as NT (Transport)
import Prelude hiding (catch)

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import TestUtils


testAsyncPoll :: TestResult (AsyncResult ()) -> Process ()
testAsyncPoll result = do
    hAsync <- async $ asyncDo $ do "go" <- expect; say "running" >> return ()
    ar <- poll hAsync
    case ar of
      AsyncPending ->
        send (_asyncWorker hAsync) "go" >> wait hAsync >>= stash result
      _ -> stash result ar >> return ()

testAsyncCancel :: TestResult (AsyncResult ()) -> Process ()
testAsyncCancel result = do
    hAsync <- async $ asyncDo $ runTestProcess $ say "running" >> return ()
    sleep $ milliSeconds 100

    p <- poll hAsync -- nasty kind of assertion: use assertEquals?
    case p of
        AsyncPending -> cancel hAsync >> wait hAsync >>= stash result
        _            -> say (show p) >> stash result p

testAsyncCancelWait :: TestResult (Maybe (AsyncResult ())) -> Process ()
testAsyncCancelWait result = do
    testPid <- getSelfPid
    p <- spawnLocal $ do
      hAsync <- async $ asyncDo $ runTestProcess $ sleep $ seconds 60
      sleep $ milliSeconds 100

      send testPid "running"

      AsyncPending <- poll hAsync
      cancelWait hAsync >>= send testPid
      
    "running" <- expect
    d <- expectTimeout (asTimeout $ seconds 5)
    case d of
        Nothing -> kill p "timed out" >> stash result Nothing
        Just ar -> stash result (Just ar)

testAsyncWaitTimeout :: TestResult (Maybe (AsyncResult ())) -> Process ()
testAsyncWaitTimeout result =
    let delay = seconds 1
    in do
    hAsync <- async $ asyncDo $ sleep $ seconds 20
    waitTimeout delay hAsync >>= stash result
    cancelWait hAsync >> return ()

testAsyncWaitTimeoutCompletes :: TestResult (Maybe (AsyncResult ()))
                              -> Process ()
testAsyncWaitTimeoutCompletes result =
    let delay = seconds 1
    in do
    hAsync <- async $ asyncDo $ sleep $ seconds 20
    waitTimeout delay hAsync >>= stash result
    cancelWait hAsync >> return ()


testAsyncWaitTimeoutSTM :: TestResult (Maybe (AsyncResult ())) -> Process ()
testAsyncWaitTimeoutSTM result =
    let delay = seconds 1
    in do
    hAsync <- async $ asyncDo $ sleep $ seconds 20  
    waitTimeoutSTM delay hAsync >>= stash result

testAsyncWaitTimeoutCompletesSTM :: TestResult (Maybe (AsyncResult Int))
                                 -> Process ()
testAsyncWaitTimeoutCompletesSTM result =
    let delay = seconds 1 in do
    
    hAsync <- async $ asyncDo $ do
        i <- expect
        return i

    r <- waitTimeoutSTM delay hAsync    
    case r of
        Nothing -> send (_asyncWorker hAsync) (10 :: Int)
                    >> wait hAsync >>= stash result . Just
        Just _  -> cancelWait hAsync >> stash result Nothing

testAsyncLinked :: TestResult Bool -> Process ()
testAsyncLinked result = do
    mv :: MVar (AsyncSTM ()) <- liftIO $ newEmptyMVar
    pid <- spawnLocal $ do
        -- NB: async == asyncLinked for AsyncChan
        h <- asyncLinked $ asyncDo $ do
            "waiting" <- expect
            return ()
        stash mv h
        "sleeping" <- expect
        return ()

    hAsync <- liftIO $ takeMVar mv

    mref <- monitor $ _asyncWorker hAsync
    exit pid "stop"

    _ <- receiveTimeout (after 5 Seconds) [
              matchIf (\(ProcessMonitorNotification mref' _ _) -> mref == mref')
                      (\_ -> return ())
            ]

    -- since the initial caller died and we used 'asyncLinked', the async should
    -- pick up on the exit signal and set the result accordingly. trying to match
    -- on 'DiedException String' is pointless though, as the *string* is highly
    -- context dependent.
    r <- waitTimeoutSTM (within 3 Seconds) hAsync
    case r of
        Nothing -> stash result True
        Just _  -> stash result False

testAsyncWaitAny :: TestResult [AsyncResult String] -> Process ()
testAsyncWaitAny result = do
  p1 <- async $ asyncDo $ expect >>= return
  p2 <- async $ asyncDo $ expect >>= return
  p3 <- async $ asyncDo $ expect >>= return
  send (_asyncWorker p3) "c"
  r1 <- waitAny [p1, p2, p3]
  
  send (_asyncWorker p1) "a"
  send (_asyncWorker p2) "b"
  sleep $ seconds 1
  
  r2 <- waitAny [p2, p3]
  r3 <- waitAny [p1, p2, p3]
  
  stash result $ [r1, r2, r3]

testAsyncWaitAnyTimeout :: TestResult (Maybe (AsyncResult String)) -> Process ()
testAsyncWaitAnyTimeout result = do
  p1 <- asyncLinked $ asyncDo $ expect >>= return
  p2 <- asyncLinked $ asyncDo $ expect >>= return
  p3 <- asyncLinked $ asyncDo $ expect >>= return
  waitAnyTimeout (seconds 1) [p1, p2, p3] >>= stash result

testAsyncCancelWith :: TestResult Bool -> Process ()
testAsyncCancelWith result = do
  p1 <- async $ asyncDo $ do { s :: String <- expect; return s }
  cancelWith "foo" p1
  AsyncFailed (DiedException _) <- wait p1
  stash result True

tests :: LocalNode  -> [Test]
tests localNode = [
    testGroup "Handling async results" [
          testCase "testAsyncCancel"
            (delayedAssertion
             "expected async task to have been cancelled"
             localNode (AsyncCancelled) testAsyncCancel)
        , testCase "testAsyncPoll"
            (delayedAssertion
             "expected poll to return a valid AsyncResult"
             localNode (AsyncDone ()) testAsyncPoll)
        , testCase "testAsyncCancelWait"
            (delayedAssertion
             "expected cancelWait to complete some time"
             localNode (Just AsyncCancelled) testAsyncCancelWait)
        , testCase "testAsyncWaitTimeout"
            (delayedAssertion
             "expected waitTimeout to return Nothing when it times out"
             localNode (Nothing) testAsyncWaitTimeout)
        , testCase "testAsyncWaitTimeoutSTM"
            (delayedAssertion
             "expected waitTimeoutSTM to return Nothing when it times out"
             localNode (Nothing) testAsyncWaitTimeoutSTM)
        , testCase "testAsyncWaitTimeoutCompletesSTM"
            (delayedAssertion
             "expected waitTimeout to return a value"
             localNode (Just (AsyncDone 10)) testAsyncWaitTimeoutCompletesSTM)
        , testCase "testAsyncLinked"
            (delayedAssertion
             "expected linked process to die with originator"
             localNode True testAsyncLinked)
        , testCase "testAsyncWaitAny"
            (delayedAssertion
             "expected waitAny to pick the first result each time"
             localNode [AsyncDone "c",
                        AsyncDone "b",
                        AsyncDone "a"] testAsyncWaitAny)
        , testCase "testAsyncWaitAnyTimeout"
            (delayedAssertion
             "expected waitAnyTimeout to handle pending results properly"
             localNode Nothing testAsyncWaitAnyTimeout)
        , testCase "testAsyncCancelWith"
            (delayedAssertion
             "expected the worker to have been killed with the given signal"
             localNode True testAsyncCancelWith)
      ]
  ]

asyncStmTests :: NT.Transport -> IO [Test]
asyncStmTests transport = do
  localNode <- newLocalNode transport initRemoteTable
  let testData = tests localNode
  return testData
