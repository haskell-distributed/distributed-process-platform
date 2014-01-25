{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE TemplateHaskell     #-}

module Main where

import Control.Concurrent.MVar (newEmptyMVar, takeMVar)
import Control.Concurrent.Utils (Lock, Exclusive(..), Synchronised(..))
import Control.Distributed.Process
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform
  ( awaitExit
  , spawnSignalled
  , Killable(..)
  )
import Control.Distributed.Process.Platform.Service.Registry
  ( Registry(..)
  , Keyable
  , KeyUpdateEvent(..)
  , RegistryKeyMonitorNotification(..)
  , addName
  , addProperty
  , giveAwayName
  , registerName
  , registerValue
  , unregisterName
  , lookupName
  , lookupProperty
  , registeredNames
  , foldNames
  , queryNames
  , findByProperty
  , findByPropertyValue
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

myRegistry :: Process (Registry String ())
myRegistry = Registry.start

counterReg :: Process (Registry String Int)
counterReg = Registry.start

withRegistry :: LocalNode
             -> (Registry String () -> Process ())
             -> Assertion
withRegistry node proc = do
  runProcess node $ do
    reg' <- myRegistry
    (proc reg') `finally` (killProc reg' "goodbye")

testAddLocalName :: TestResult RegisterKeyReply -> Process ()
testAddLocalName result = do
  reg <- myRegistry
  stash result =<< addName reg "foobar"

testAddLocalProperty :: TestResult (Maybe Int) -> Process ()
testAddLocalProperty result = do
  reg <- counterReg
  addProperty reg "chickens" (42 :: Int)
  stash result =<< lookupProperty reg "chickens"

testAddRemoteProperty :: TestResult Int -> Process ()
testAddRemoteProperty result = do
  reg <- counterReg
  p <- spawnLocal $ do
    pid <- expect
    Just i <- lookupProperty reg "ducks" :: Process (Maybe Int)
    send pid i
  RegisteredOk <- registerValue reg p "ducks" (39 :: Int)
  getSelfPid >>= send p
  expect >>= stash result

testFindByPropertySet :: TestResult Bool -> Process ()
testFindByPropertySet result = do
  reg <- counterReg
  p1 <- spawnLocal $ addProperty reg "animals" (1 :: Int) >> expect >>= return
  p2 <- spawnLocal $ addProperty reg "animals" (1 :: Int) >> expect >>= return
  sleep $ seconds 1
  found <- findByProperty reg "animals"
  found `shouldContain` p1
  found `shouldContain` p2
  notFound <- findByProperty reg "foobar"
  stash result $ length notFound == 0

testFindByPropertyValueSet :: TestResult Bool -> Process ()
testFindByPropertyValueSet result = do
  us <- getSelfPid
  reg <- counterReg
  p1 <- spawnLocal $ link us >> addProperty reg "animals" (1 :: Int) >> expect >>= return
  _  <- spawnLocal $ link us >> addProperty reg "animals" (2 :: Int) >> expect >>= return
  p3 <- spawnLocal $ link us >> addProperty reg "animals" (1 :: Int) >> expect >>= return
  _  <- spawnLocal $ link us >> addProperty reg "ducks"   (1 :: Int) >> expect >>= return

  sleep $ seconds 1
  found <- findByPropertyValue reg "animals" (1 :: Int)
  found `shouldContain` p1
  found `shouldContain` p3
  stash result $ length found == 2

testCheckLocalName :: Registry String () -> Process ()
testCheckLocalName reg = do
  void $ addName reg "fwibble"
  fwibble <- lookupName reg "fwibble"
  selfPid <- getSelfPid
  fwibble `shouldBe` equalTo (Just selfPid)

testGiveAwayName :: Registry String () -> Process ()
testGiveAwayName reg = do
  testPid <- getSelfPid
  void $ addName reg "cat"
  pid <- spawnLocal $ link testPid >> (expect :: Process ())
  giveAwayName reg "cat" pid
  cat <- lookupName reg "cat"
  cat `shouldBe` equalTo (Just pid)

testMultipleRegistrations :: Registry String () -> Process ()
testMultipleRegistrations reg = do
  self <- getSelfPid
  forM_ names (addName reg)
  forM_ names $ \name -> do
    found <- lookupName reg name
    found `shouldBe` equalTo (Just self)
  where
    names = ["foo", "bar", "baz"]

testDuplicateRegistrations :: Registry String () -> Process ()
testDuplicateRegistrations reg = do
  void $ addName reg "foobar"
  RegisteredOk <- addName reg "foobar"
  pid <- spawnLocal $ (expect :: Process ()) >>= return
  result <- registerName reg "foobar" pid
  result `shouldBe` equalTo AlreadyRegistered

testUnregisterName :: Registry String () -> Process ()
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

testUnregisterUnknownName :: Registry String () -> Process ()
testUnregisterUnknownName reg = do
  result <- unregisterName reg "no.such.name"
  result `shouldBe` equalTo UnregisterKeyNotFound

testUnregisterAnothersName :: Registry String () -> Process ()
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

testProcessDeathHandling :: Registry String () -> Process ()
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

testLocalRegNamesFold :: Registry String () -> Process ()
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

testLocalQueryNamesFold :: Registry String () -> Process ()
testLocalQueryNamesFold reg = do
  pids <- forM [1..1000] $ \(i :: Int) -> spawnLocal $ do
    addName reg (show i) >> expect :: Process ()
  waitRegs reg 1000
  ns <- queryNames reg $ \(sh :: SearchHandle String ProcessId) -> do
    return $ Foldable.foldl (\acc pid -> (pid:acc)) [] sh
  (List.sort ns) `shouldBe` equalTo pids
  where
    waitRegs :: Registry String () -> Int -> Process ()
    waitRegs _    0 = return ()
    waitRegs reg' n = await reg' (show n) >> waitRegs reg' (n - 1)

testMonitorName :: Registry String () -> Process ()
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
      matchIf (\(RegistryKeyMonitorNotification k ref _ _) ->
                k == "proc.name.2" && ref == mRef)
              (\(RegistryKeyMonitorNotification _ _ ev _) -> return ev)
    ]
  res `shouldBe` equalTo (Just (KeyOwnerDied DiedNormal))

testMonitorNameChange :: Registry String () -> Process ()
testMonitorNameChange reg = do
  let k = "proc.name.foo"

  lock <- liftIO $ new :: Process Lock
  acquire lock
  testPid <- getSelfPid

  pid <- spawnSignalled (addName reg k) $ const $ do
    link testPid
    synchronised lock $ giveAwayName reg k testPid
    expect >>= return

  -- at this point, we know the child has grabbed the name k for itself
  mRef <- Registry.monitorName reg k
  release lock

  ev <- receiveWait [ matchIf (\(RegistryKeyMonitorNotification k' ref _ _) ->
                                  k' == k && ref == mRef)
                                (\(RegistryKeyMonitorNotification _ _ ev' _) ->
                                  return ev') ]
  ev `shouldBe` equalTo (KeyOwnerChanged pid testPid)

testUnmonitor :: TestResult Bool -> Process ()
testUnmonitor result = do
  let k = "chickens"
  let name = "poultry"

  reg <- counterReg
  (sp, rp) <- newChan

  pid <- spawnLocal $ do
    void $ addProperty reg k (42 :: Int)
    addName reg name
    sendChan sp ()
    expect >>= return

  () <- receiveChan rp
  mRef <- Registry.monitorProp reg k pid

  void $ receiveWait [
      matchIf (\(RegistryKeyMonitorNotification k' ref ev _) ->
                k' == k && ref == mRef && ev == (KeyRegistered pid))
              return
    ]

  Registry.unmonitor reg mRef
  kill pid "goodbye!"
  awaitExit pid

  Nothing <- lookupName reg name
  t <- receiveTimeout (after 1 Seconds) [
           matchIf (\(RegistryKeyMonitorNotification k' ref ev _) ->
                    k' == k && ref == mRef && ev == (KeyRegistered pid))
                   return
         ]
  t `shouldBe` (equalTo Nothing)
  stash result True

testMonitorPropertyChanged :: TestResult Bool -> Process ()
testMonitorPropertyChanged result = do
  let k = "chickens"
  let name = "poultry"

  lock <- liftIO $ new :: Process Lock
  reg <- counterReg

  -- yes, using the lock here without exception handling is risky...
  acquire lock

  pid <- spawnSignalled (addProperty reg k (42 :: Int)) $ const $ do
    addName reg name
    synchronised lock $ addProperty reg k (45 :: Int)
    expect >>= return

  -- at this point, the worker has already registered 42 (since we used
  -- spawnSignalled to start it) and is waiting for us to unlock...
  mRef <- Registry.monitorProp reg k pid

  -- we /must/ receive a monitor notification first, for the pre-existing
  -- key and only then will we let the worker move on and update the value
  void $ receiveWait [
      matchIf (\(RegistryKeyMonitorNotification k' ref ev _) ->
                k' == k && ref == mRef && ev == (KeyRegistered pid))
              return
    ]

  release lock

  kr <- receiveTimeout 1000000 [
      matchIf (\(RegistryKeyMonitorNotification k' ref ev _) ->
                k' == k && ref == mRef && ev == (KeyRegistered pid))
              return
    ]

  stash result (kr /= Nothing)

testMonitorPropertyOwnerDied :: Registry String () -> Process ()
testMonitorPropertyOwnerDied reg = do
  let k = "foo.bar"
  pid <- spawnSignalled (addProperty reg k ()) $ const $ do
    expect >>= return

  mRef <- Registry.monitorProp reg k pid
  kill pid "goodbye!"

  -- we /must/ receive a monitor notification first, for the pre-existing
  -- key and only then will we let the worker move on and update the value
  r <- receiveTimeout 1000000 [
      matchIf (\(RegistryKeyMonitorNotification k' ref ev _) ->
                k' == k && ref == mRef && ev /= (KeyRegistered pid))
              (\(RegistryKeyMonitorNotification _ _ ev _) ->
                return ev)
    ]
  case r of
    Just (KeyOwnerDied (DiedException _)) -> return ()
    _ -> (liftIO $ putStrLn (show r)) >> return ()

-- testMonitorLeaseExpired :: ProcessId -> Process ()

-- testMonitorPropertyLeaseExpired :: ProcessId -> Process ()

testMonitorUnregistration :: Registry String () -> Process ()
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
      matchIf (\(RegistryKeyMonitorNotification k ref _ _) ->
                k == "proc.1" && ref == mRef)
              (\(RegistryKeyMonitorNotification _ _ ev _) -> return ev)
    ]
  res `shouldBe` equalTo (Just KeyUnregistered)

testMonitorRegistration :: Registry String () -> Process ()
testMonitorRegistration reg = do
  kRef <- Registry.monitorName reg "my.proc"
  pid <- spawnLocal $ do
    void $ addName reg "my.proc"
    expect :: Process ()
  res <- receiveTimeout (after 2 Seconds) [
      matchIf (\(RegistryKeyMonitorNotification k ref _ _) ->
                k == "my.proc" && ref == kRef)
              (\(RegistryKeyMonitorNotification _ _ ev _) -> return ev)
    ]
  res `shouldBe` equalTo (Just (KeyRegistered pid))

testAwaitRegistration :: Registry String () -> Process ()
testAwaitRegistration reg = do
  pid <- spawnLocal $ do
    void $ addName reg "foo.bar"
    expect :: Process ()
  res <- awaitTimeout reg (Delay $ within 5 Seconds) "foo.bar"
  res `shouldBe` equalTo (RegisteredName pid "foo.bar")

testAwaitRegistrationNoTimeout :: Registry String () -> Process ()
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

testAwaitServerDied :: Registry String () -> Process ()
testAwaitServerDied reg = do
  result <- liftIO $ newEmptyMVar
  _ <- spawnLocal $ await reg "bobobob" >>= stash result
  sleep $ milliSeconds 250
  killProc reg "bye!"
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
           (testProc testGiveAwayName)
        , testCase "Verified Registration"
           (testProc testCheckLocalName)
        , testCase "Single Process, Multiple Registered Names"
           (testProc testMultipleRegistrations)
        , testCase "Duplicate Registration Fails"
           (testProc testDuplicateRegistrations)
        , testCase "Unregister Own Name"
           (testProc testUnregisterName)
        , testCase "Unregister Unknown Name"
           (testProc testUnregisterUnknownName)
        , testCase "Unregister Someone Else's Name"
           (testProc testUnregisterAnothersName)
        ]
      , testGroup "Properties"
        [
          testCase "Simple Property Registration"
           (delayedAssertion
            "expected the server to return the property value 42"
            localNode (Just 42) testAddLocalProperty)
        , testCase "Remote Property Registration"
           (delayedAssertion
            "expected the server to return the property value 39"
            localNode 39 testAddRemoteProperty)
        ]
      , testGroup "Queries"
        [
          testCase "Folding Over Registered Names (Locally)"
           (testProc testLocalRegNamesFold)
        , testCase "Querying Registered Names (Locally)"
           (testProc testLocalQueryNamesFold)
        , testCase "Querying Process Where Property Exists (Locally)"
           (delayedAssertion
            "expected the server to return only the relevant processes"
            localNode True testFindByPropertySet)
        , testCase "Querying Process Where Property Is Set To Specific Value (Locally)"
           (delayedAssertion
            "expected the server to return only the relevant processes"
            localNode True testFindByPropertyValueSet)
        ]
      , testGroup "Named Process Monitoring/Tracking"
        [
          testCase "Process Death Results In Unregistration"
           (testProc testProcessDeathHandling)
        , testCase "Monitoring Name Changes"
           (testProc testMonitorName)
        , testCase "Monitoring Name Changes (KeyOwnerChanged)"
           (testProc testMonitorNameChange)
        , testCase "Unmonitoring (Ignoring) Changes"
          (delayedAssertion
           "expected no further notifications after 'unmonitor' was called"
           localNode True testUnmonitor)
        , testCase "Monitoring Property Changes/Updates"
          (delayedAssertion
           "expected the server to send additional notifications for each change"
           localNode True testMonitorPropertyChanged)
        , testCase "Monitoring Property Owner Death"
           (testProc testMonitorPropertyOwnerDied)
        , testCase "Monitoring Registration"
           (testProc testMonitorRegistration)
        , testCase "Awaiting Registration"
           (testProc testAwaitRegistration)
        , testCase "Await without timeout"
           (testProc testAwaitRegistrationNoTimeout)
        , testCase "Server Died During Await"
           (testProc testAwaitServerDied)
        , testCase "Monitoring Unregistration"
           (testProc testMonitorUnregistration)
        ]
    ]

main :: IO ()
main = testMain $ tests

