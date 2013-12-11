{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE TemplateHaskell     #-}

module Main where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform hiding (__remoteTable, monitor,
                                                    send, nsend, sendChan)
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess
import Control.Distributed.Process.Platform.Test
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Platform.Timer
import Control.Distributed.Process.Serializable()

import MathsDemo
import Counter
import qualified SafeCounter as SafeCounter
import Control.Distributed.Process.Platform.Task.Queue.BlockingQueue hiding (start)
import qualified Control.Distributed.Process.Platform.Task.Queue.BlockingQueue as Pool (start)

#if ! MIN_VERSION_base(4,6,0)
import Prelude hiding (catch)
#endif

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import TestUtils
import ManagedProcessCommon

import qualified Network.Transport as NT
import Control.Monad (void)

-- utilities

sampleTask :: (TimeInterval, String) -> Process String
sampleTask (t, s) = sleep t >> return s

namedTask :: (String, String) -> Process String
namedTask (name, result) = do
  self <- getSelfPid
  register name self
  () <- expect
  return result

crashingTask :: SendPort ProcessId -> Process String
crashingTask sp = getSelfPid >>= sendChan sp >> die "Boom"

$(remotable ['sampleTask, 'namedTask, 'crashingTask])

-- SimplePool tests

startPool :: SizeLimit -> Process ProcessId
startPool sz = spawnLocal $ do
  Pool.start (pool sz :: Process (InitResult (BlockingQueue String)))

testSimplePoolJobBlocksCaller :: TestResult (AsyncResult (Either ExitReason String))
                              -> Process ()
testSimplePoolJobBlocksCaller result = do
  pid <- startPool 1
  -- we do a non-blocking test first
  job <- return $ ($(mkClosure 'sampleTask) (seconds 2, "foobar"))
  callAsync pid job >>= wait >>= stash result

testJobQueueSizeLimiting ::
    TestResult (Maybe (AsyncResult (Either ExitReason String)),
                Maybe (AsyncResult (Either ExitReason String)))
                         -> Process ()
testJobQueueSizeLimiting result = do
  pid <- startPool 1
  job1 <- return $ ($(mkClosure 'namedTask) ("job1", "foo"))
  job2 <- return $ ($(mkClosure 'namedTask) ("job2", "bar"))
  h1 <- callAsync pid job1 :: Process (Async (Either ExitReason String))
  h2 <- callAsync pid job2 :: Process (Async (Either ExitReason String))

  -- despite the fact that we tell job2 to proceed first,
  -- the size limit (of 1) will ensure that only job1 can
  -- proceed successfully!
  nsend "job2" ()
  AsyncPending <- poll h2
  Nothing <- whereis "job2"

  -- we can get here *very* fast, so give the registration time to kick in
  sleep $ milliSeconds 250
  j1p <- whereis "job1"
  case j1p of
    Nothing -> die $ "timing is out - job1 isn't registered yet"
    Just p  -> send p ()

  -- once job1 completes, we *should* be able to proceed with job2
  -- but we allow a little time for things to catch up
  sleep $ milliSeconds 250
  nsend "job2" ()

  r2 <- waitTimeout (within 2 Seconds) h2
  r1 <- waitTimeout (within 2 Seconds) h1
  stash result (r1, r2)

testExecutionErrors :: TestResult Bool -> Process ()
testExecutionErrors result = do
    pid <- startPool 1
    (sp, rp) <- newChan :: Process (SendPort ProcessId,
                                    ReceivePort ProcessId)
    job <- return $ ($(mkClosure 'crashingTask) sp)
    res <- executeTask pid job
    rpid <- receiveChan rp
--  liftIO $ putStrLn (show res)
    stash result (expectedErrorMessage rpid == res)
  where
    expectedErrorMessage p =
      Left $ ExitOther $ "DiedException \"exit-from=" ++ (show p) ++ ",reason=Boom\""

myRemoteTable :: RemoteTable
myRemoteTable = Main.__remoteTable initRemoteTable

tests :: NT.Transport  -> IO [Test]
tests transport = do
  localNode <- newLocalNode transport myRemoteTable
  return [
    testGroup "Task Execution And Prioritisation" [
         testCase "Each execution blocks the submitter"
         (delayedAssertion
          "expected the server to return the task outcome"
          localNode (AsyncDone (Right "foobar")) testSimplePoolJobBlocksCaller)
       , testCase "Only 'max' tasks can proceed at any time"
         (delayedAssertion
          "expected the server to block the second job until the first was released"
          localNode
          (Just (AsyncDone (Right "foo")),
           Just (AsyncDone (Right "bar"))) testJobQueueSizeLimiting)
       , testCase "Crashing Tasks are Reported Properly"
         (delayedAssertion
          "expected the server to report an error"
          localNode True testExecutionErrors)
       ]
    ]

main :: IO ()
main = testMain $ tests

