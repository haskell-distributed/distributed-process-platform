{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module TestAsyncSTM where

import Control.Distributed.Process
import Control.Distributed.Process.Node
import Control.Distributed.Process.Serializable()
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.Async.AsyncSTM
import Data.Binary()
import Data.Typeable()
import qualified Network.Transport as NT (Transport)
import Prelude hiding (catch)

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import Control.Distributed.Process.Platform.Test
import TestUtils

testAsyncPoll :: TestResult (AsyncResult ()) -> Process ()
testAsyncPoll result = do
    hAsync <- async $ do "go" <- expect; say "running" >> return ()
    ar <- poll hAsync
    case ar of
      AsyncPending ->
        send (_asyncWorker hAsync) "go" >> wait hAsync >>= stash result
      _ -> stash result ar >> return ()
      
tests :: LocalNode  -> [Test]
tests localNode = [
    testGroup "Handling async results" [
          testCase "testAsyncPoll"
            (delayedAssertion
             "expected poll to return a valid AsyncResult"
             localNode (AsyncDone ()) testAsyncPoll) 
      ]
  ]

asyncStmTests :: NT.Transport -> IO [Test]
asyncStmTests transport = do
  localNode <- newLocalNode transport initRemoteTable
  let testData = tests localNode
  return testData
