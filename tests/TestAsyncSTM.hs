{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module TestAsyncSTM where

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
    hAsync <- async $ do "go" <- expect; say "running" >> return ()
    ar <- poll hAsync
    case ar of
      AsyncPending ->
        send (_asyncWorker hAsync) "go" >> wait hAsync >>= stash result
      _ -> stash result ar >> return ()

testAsyncCancel :: TestResult (AsyncResult ()) -> Process ()
testAsyncCancel result = do
    hAsync <- async $ runTestProcess $ say "running" >> return ()
    sleep $ milliSeconds 100

    p <- poll hAsync -- nasty kind of assertion: use assertEquals?
    case p of
        AsyncPending -> cancel hAsync >> wait hAsync >>= stash result
        _            -> say (show p) >> stash result p

testAsyncCancelWait :: TestResult (Maybe (AsyncResult ())) -> Process ()
testAsyncCancelWait result = do
    testPid <- getSelfPid
    p <- spawnLocal $ do
      hAsync <- async $ runTestProcess $ say "running" >> (sleep $ seconds 60)
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
    hAsync <- async $ sleep $ seconds 20
    waitTimeout delay hAsync >>= stash result
    cancelWait hAsync >> return ()

testAsyncWaitTimeoutCompletes :: TestResult (Maybe (AsyncResult ()))
                              -> Process ()
testAsyncWaitTimeoutCompletes result =
    let delay = seconds 1
    in do
    hAsync <- async $ sleep $ seconds 20
    waitTimeout delay hAsync >>= stash result
    cancelWait hAsync >> return ()


testAsyncWaitTimeoutSTM :: TestResult (Maybe (AsyncResult ())) -> Process ()
testAsyncWaitTimeoutSTM result =
    let delay = seconds 1
    in do
    hAsync <- async $ sleep $ seconds 20  
    r <- waitTimeoutSTM delay hAsync
    liftIO $ putStrLn $ "result: " ++ (show r)
    stash result r

testAsyncWaitTimeoutCompletesSTM :: TestResult (Maybe (AsyncResult Int))
                                 -> Process ()
testAsyncWaitTimeoutCompletesSTM result =
    let delay = seconds 1 in do
    
    hAsync <- async $ do
        i <- expect
        return i

    r <- waitTimeoutSTM delay hAsync    
    case r of
        Nothing -> send (_asyncWorker hAsync) (10 :: Int)
                    >> wait hAsync >>= stash result . Just
        Just _  -> cancelWait hAsync >> stash result Nothing

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
             localNode (Nothing) testAsyncWaitTimeoutCompletesSTM)
        , testCase "testAsyncWaitTimeoutCompletesSTM"
            (delayedAssertion
             "expected waitTimeout to return a value"
             localNode (Just (AsyncDone 10)) testAsyncWaitTimeoutCompletesSTM)
      ]
  ]

asyncStmTests :: NT.Transport -> IO [Test]
asyncStmTests transport = do
  localNode <- newLocalNode transport initRemoteTable
  let testData = tests localNode
  return testData
