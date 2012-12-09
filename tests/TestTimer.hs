{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE TemplateHaskell           #-}

module TestTimer where

import Prelude hiding (catch)
import Data.Binary (Binary(..))
import Data.Typeable (Typeable)
import Data.DeriveTH
import Control.Monad (forever)
import Control.Concurrent.MVar
  ( MVar
  , newEmptyMVar
  , putMVar
  , takeMVar
  , withMVar
  )
-- import Control.Applicative ((<$>), (<*>), pure, (<|>))
import qualified Network.Transport as NT (Transport)
import Network.Transport.TCP (TransportInternals(socketBetween))
import Control.Distributed.Process
import Control.Distributed.Process.Internal.Types()
import Control.Distributed.Process.Node
import Control.Distributed.Process.Serializable()
import Control.Distributed.Platform.Timer

import Test.HUnit (Assertion)
import Test.Framework (Test, defaultMain, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import Test.HUnit.Base (assertBool)

type TestResult a = MVar a

data Ping = Ping
    deriving (Typeable)
$(derive makeBinary ''Ping)

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

testTimerFlush :: TestResult Bool -> Process ()
testTimerFlush result = do
  let delay = seconds 1
  self <- getSelfPid
  ref  <- ticker delay self
  _    <- monitor ref
  
  -- sleep so we *should* have a message in our 'mailbox'
  sleep $ milliseconds 1500
  
  -- flush it out if it's there
  flushTimer ref Tick (seconds 3)
  
  m <- expectTimeout 10
  case m of
      Nothing   -> stash result True
      Just Tick -> stash result False

--------------------------------------------------------------------------------
-- Utilities and Plumbing                                                     --
--------------------------------------------------------------------------------

delayedAssertion :: (Eq a) => String -> LocalNode -> a ->
                    (TestResult a -> Process ()) -> Assertion
delayedAssertion note localNode expected testProc = do
  result <- newEmptyMVar
  _ <- forkProcess localNode $ testProc result
  assertComplete note result expected

assertComplete :: (Eq a) => String -> MVar a -> a -> IO ()
assertComplete msg mv a = do
    b <- takeMVar mv
    assertBool msg (a == b)

noop :: Process ()
noop = return ()

stash :: TestResult a -> a -> Process ()
stash mvar x = liftIO $ putMVar mvar x

tests :: LocalNode  -> [Test]
tests localNode = [
    testGroup "Timer Tests" [
        testCase "testSendAfter"    (delayedAssertion
                                     "expected Ping within 1 second"
                                     localNode True testSendAfter)
      , testCase "testRunAfter"     (delayedAssertion
                                     "expecting run (which pings parent) within 2 seconds"
                                     localNode True testRunAfter)
      , testCase "testCancelTimer"  (delayedAssertion
                                     "expected cancelTimer to exit the timer process normally"
                                     localNode
                                     True
                                     testCancelTimer)
      , testCase "testPeriodicSend" (delayedAssertion
                                     "expected ten Ticks to have been sent before exiting"
                                     localNode True testPeriodicSend)
      , testCase "testTimerReset"   (delayedAssertion
                                     "expected no Ticks to have been sent before resetting"
                                     localNode 0 testTimerReset)
      , testCase "testTimerFlush"   (delayedAssertion
                                     "expected all Ticks to have been flushed"
                                     localNode True testTimerFlush)
      ]
  ]

timerTests :: NT.Transport -> TransportInternals -> IO [Test]
timerTests transport _ = do
  localNode <- newLocalNode transport initRemoteTable
  let testData = tests localNode
  return testData
