{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE TemplateHaskell     #-}

module Main where

import Control.Concurrent.MVar (newEmptyMVar, takeMVar)
import Control.Distributed.Process
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform.Service.Registry
  ( Registry(..)
  , Keyable
  , KeyUpdateEventMask(..)
  , KeyUpdateEvent(..)
  , RegistryKeyMonitorNotification(..)
  , addName
  , giveAwayName
  , registerName
  , unregisterName
  , lookupName
  , registeredNames
  , foldNames
  , queryNames
  , awaitTimeout
  , await
  , SearchHandle
  , AwaitResult(..)
  , RegisterKeyReply(..)
  , UnregisterKeyReply(..)
  )
import qualified Control.Distributed.Process.Platform.Service.Registry as Registry
import Control.Distributed.Process.Platform.Test
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Platform.Timer (sleep)
import Control.Distributed.Process.Serializable
import Control.Monad (void, forM_, forM)
import Control.Rematch
  ( equalTo
  )

import qualified Data.Foldable as Foldable
import qualified Data.List as List

#if ! MIN_VERSION_base(4,6,0)
import Prelude hiding (catch)
#endif

import Test.HUnit (Assertion, assertFailure)
import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import TestUtils

import qualified Network.Transport as NT

myRegistry :: Registry String ()
myRegistry = Registry

withRegistry :: forall k v. (Keyable k, Serializable v)
             => LocalNode
             -> Registry k v
             -> (ProcessId -> Process ())
             -> Assertion
withRegistry node reg proc = do
  runProcess node $ do
    reg' <- Registry.start reg
    (proc reg') `finally` (kill reg' "goodbye")

testAddLocalName :: TestResult RegisterKeyReply -> Process ()
testAddLocalName result = do
  reg <- Registry.start myRegistry
  stash result =<< addName reg "foobar"

testCheckLocalName :: ProcessId -> Process ()
testCheckLocalName reg = do
  void $ addName reg "fwibble"
  fwibble <- lookupName reg "fwibble"
  selfPid <- getSelfPid
  fwibble `shouldBe` equalTo (Just selfPid)

testGiveAwayName :: ProcessId -> Process ()
testGiveAwayName reg = do
  testPid <- getSelfPid
  void $ addName reg "cat"
  pid <- spawnLocal $ link testPid >> (expect :: Process ())
  giveAwayName reg "cat" pid
  cat <- lookupName reg "cat"
  cat `shouldBe` equalTo (Just pid)

testMultipleRegistrations :: ProcessId -> Process ()
testMultipleRegistrations reg = do
  self <- getSelfPid
  forM_ names (addName reg)
  forM_ names $ \name -> do
    found <- lookupName reg name
    found `shouldBe` equalTo (Just self)
  where
    names = ["foo", "bar", "baz"]

testDuplicateRegistrations :: ProcessId -> Process ()
testDuplicateRegistrations reg = do
  void $ addName reg "foobar"
  RegisteredOk <- addName reg "foobar"
  pid <- spawnLocal $ (expect :: Process ()) >>= return
  result <- registerName reg "foobar" pid
  result `shouldBe` equalTo AlreadyRegistered

testUnregisterName :: ProcessId -> Process ()
testUnregisterName reg = do
  self <- getSelfPid
  void $ addName reg "fwibble"
  void $ addName reg "fwobble"
  Just self' <- lookupName reg "fwibble"
  self' `shouldBe` equalTo self
  unreg <- unregisterName reg "fwibble"
  unregFwibble <- lookupName reg "fwibble"
  unreg `shouldBe` equalTo UnregisterOk
  -- fwibble is gone...
  unregFwibble `shouldBe` equalTo Nothing
  -- but fwobble is still registered
  fwobble <- lookupName reg "fwobble"
  fwobble `shouldBe` equalTo (Just self)

testUnregisterUnknownName :: ProcessId -> Process ()
testUnregisterUnknownName reg = do
  result <- unregisterName reg "no.such.name"
  result `shouldBe` equalTo UnregisterKeyNotFound

testUnregisterAnothersName :: ProcessId -> Process ()
testUnregisterAnothersName reg = do
  (sp, rp) <- newChan
  pid <- spawnLocal $ do
    void $ addName reg "proc.name"
    sendChan sp ()
    (expect :: Process ()) >>= return
  () <- receiveChan rp
  found <- lookupName reg "proc.name"
  found `shouldBe` equalTo (Just pid)
  unreg <- unregisterName reg "proc.name"
  unreg `shouldBe` equalTo UnregisterInvalidKey

testProcessDeathHandling :: ProcessId -> Process ()
testProcessDeathHandling reg = do
  (sp, rp) <- newChan
  pid <- spawnLocal $ do
    void $ addName reg "proc.name.1"
    void $ addName reg "proc.name.2"
    sendChan sp ()
    (expect :: Process ()) >>= return
  () <- receiveChan rp
  void $ monitor pid
  regNames <- registeredNames reg pid
  regNames `shouldContain` "proc.name.1"
  regNames `shouldContain` "proc.name.2"
  send pid ()
  receiveWait [
      match (\(ProcessMonitorNotification _ _ _) -> return ())
    ]
  forM_ [1..2 :: Int] $ \n -> do
    let name = "proc.name." ++ (show n)
    found <- lookupName reg name
    found `shouldBe` equalTo Nothing
  regNames' <- registeredNames reg pid
  regNames' `shouldBe` equalTo ([] :: [String])

testLocalRegNamesFold :: ProcessId -> Process ()
testLocalRegNamesFold reg = do
  parent <- getSelfPid
  forM_ [1..1000] $ \(i :: Int) -> spawnLocal $ do
    send parent i
    addName reg (show i) >> expect :: Process ()
  waitForTestRegistrations
  ns <- foldNames reg [] $ \acc (n, _) -> return ((read n :: Int):acc)
  (List.sort ns) `shouldBe` equalTo [1..1000]
  where
    waitForTestRegistrations = do
      void $ receiveWait [ matchIf (\(i :: Int) -> i == 1000) (\_ -> return ()) ]
      sleep $ milliSeconds 150

testLocalQueryNamesFold :: ProcessId -> Process ()
testLocalQueryNamesFold reg = do
  pids <- forM [1..1000] $ \(i :: Int) -> spawnLocal $ do
    addName reg (show i) >> expect :: Process ()
  waitRegs reg 1000
  ns <- queryNames reg $ \(sh :: SearchHandle String ProcessId) -> do
    return $ Foldable.foldl (\acc pid -> (pid:acc)) [] sh
  (List.sort ns) `shouldBe` equalTo pids
  where
    waitRegs :: ProcessId -> Int -> Process ()
    waitRegs _    0 = return ()
    waitRegs reg' n = await reg' (show n) >> waitRegs reg' (n - 1)

testMonitorName :: ProcessId -> Process ()
testMonitorName reg = do
  (sp, rp) <- newChan
  pid <- spawnLocal $ do
    void $ addName reg "proc.name.1"
    void $ addName reg "proc.name.2"
    sendChan sp ()
    (expect :: Process ()) >>= return
  () <- receiveChan rp
  mRef <- Registry.monitorName reg "proc.name.2"
  send pid ()
  res <- receiveTimeout (after 2 Seconds) [
      matchIf (\(RegistryKeyMonitorNotification k ref _) ->
                k == "proc.name.2" && ref == mRef)
              (\(RegistryKeyMonitorNotification _ _ ev) -> return ev)
    ]
  res `shouldBe` equalTo (Just (KeyOwnerDied DiedNormal))

testMonitorUnregistration :: ProcessId -> Process ()
testMonitorUnregistration reg = do
  (sp, rp) <- newChan
  pid <- spawnLocal $ do
    void $ addName reg "proc.1"
    sendChan sp ()
    expect :: Process ()
    void $ unregisterName reg "proc.1"
    sendChan sp ()
    (expect :: Process ()) >>= return
  () <- receiveChan rp
  mRef <- Registry.monitorName reg "proc.1"
  send pid ()
  () <- receiveChan rp
  res <- receiveTimeout (after 2 Seconds) [
      matchIf (\(RegistryKeyMonitorNotification k ref _) ->
                k == "proc.1" && ref == mRef)
              (\(RegistryKeyMonitorNotification _ _ ev) -> return ev)
    ]
  res `shouldBe` equalTo (Just KeyUnregistered)

testMonitorRegistration :: ProcessId -> Process ()
testMonitorRegistration reg = do
  kRef <- Registry.monitorName reg "my.proc"
  pid <- spawnLocal $ do
    void $ addName reg "my.proc"
    expect :: Process ()
  res <- receiveTimeout (after 2 Seconds) [
      matchIf (\(RegistryKeyMonitorNotification k ref _) ->
                k == "my.proc" && ref == kRef)
              (\(RegistryKeyMonitorNotification _ _ ev) -> return ev)
    ]
  res `shouldBe` equalTo (Just (KeyRegistered pid))

testAwaitRegistration :: ProcessId -> Process ()
testAwaitRegistration reg = do
  pid <- spawnLocal $ do
    void $ addName reg "foo.bar"
    expect :: Process ()
  res <- awaitTimeout reg (Delay $ within 5 Seconds) "foo.bar"
  res `shouldBe` equalTo (RegisteredName pid "foo.bar")

testAwaitRegistrationNoTimeout :: ProcessId -> Process ()
testAwaitRegistrationNoTimeout reg = do
  parent <- getSelfPid
  barrier <- liftIO $ newEmptyMVar
  void $ spawnLocal $ do
    res <- await reg "baz.bog"
    case res of
      RegisteredName _ "baz.bog" -> stash barrier ()
      _ -> kill parent "BANG!"
  void $ addName reg "baz.bog"
  liftIO $ takeMVar barrier >>= return

testAwaitServerDied :: ProcessId -> Process ()
testAwaitServerDied reg = do
  result <- liftIO $ newEmptyMVar
  _ <- spawnLocal $ await reg "bobobob" >>= stash result
  sleep $ milliSeconds 250
  kill reg "bye!"
  res <- liftIO $ takeMVar result
  case res of
    ServerUnreachable (DiedException _) -> return ()
    _ -> liftIO $ assertFailure (show res)

tests :: NT.Transport  -> IO [Test]
tests transport = do
  localNode <- newLocalNode transport initRemoteTable
  let testProc = withRegistry localNode
  return [
        testGroup "Name Registration/Unregistration"
        [
          testCase "Simple Registration"
           (delayedAssertion
            "expected the server to return the incremented state as 7"
            localNode RegisteredOk testAddLocalName)
        , testCase "Give Away Name"
           (testProc myRegistry testGiveAwayName)
        , testCase "Verified Registration"
           (testProc myRegistry testCheckLocalName)
        , testCase "Single Process, Multiple Registered Names"
           (testProc myRegistry testMultipleRegistrations)
        , testCase "Duplicate Registration Fails"
           (testProc myRegistry testDuplicateRegistrations)
        , testCase "Unregister Own Name"
           (testProc myRegistry testUnregisterName)
        , testCase "Unregister Unknown Name"
           (testProc myRegistry testUnregisterUnknownName)
        , testCase "Unregister Someone Else's Name"
           (testProc myRegistry testUnregisterAnothersName)
        ]
      , testGroup "Queries"
        [
          testCase "Folding Over Registered Names (Locally)"
           (testProc myRegistry testLocalRegNamesFold)
        , testCase "Querying Registered Names (Locally)"
           (testProc myRegistry testLocalQueryNamesFold)
        ]
      , testGroup "Named Process Monitoring/Tracking"
        [
          testCase "Process Death Results In Unregistration"
           (testProc myRegistry testProcessDeathHandling)
        , testCase "Monitoring Name Changes"
           (testProc myRegistry testMonitorName)
        , testCase "Monitoring Registration"
           (testProc myRegistry testMonitorRegistration)
        , testCase "Awaiting Registration"
           (testProc myRegistry testAwaitRegistration)
        , testCase "Await without timeout"
           (testProc myRegistry testAwaitRegistrationNoTimeout)
        , testCase "Server Died During Await"
           (testProc myRegistry testAwaitServerDied)
        , testCase "Monitoring Unregistration"
           (testProc myRegistry testMonitorUnregistration)
        ]
    ]

main :: IO ()
main = testMain $ tests

