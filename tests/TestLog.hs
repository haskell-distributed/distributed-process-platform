{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

-- import Control.Exception (SomeException)
import Control.Concurrent.MVar (MVar, newMVar, takeMVar, putMVar)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TChan
import Control.Distributed.Process hiding (monitor)
import Control.Distributed.Process.Closure (remotable, mkStaticClosure)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform hiding (__remoteTable)
import qualified Control.Distributed.Process.Platform.Service.SystemLog as Log (Logger, error)
import Control.Distributed.Process.Platform.Service.SystemLog hiding (Logger, error)
import Control.Distributed.Process.Platform.Test
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Platform.Timer
import Control.Monad (void)
import Data.List (delete)

#if ! MIN_VERSION_base(4,6,0)
import Prelude hiding (catch, drop, Read)
#else
import Prelude hiding (drop, read, Read)
#endif

import Test.Framework as TF (testGroup, Test)
import Test.Framework.Providers.HUnit
import TestUtils

import GHC.Read
import Text.ParserCombinators.ReadP as P
import Text.ParserCombinators.ReadPrec

import qualified Network.Transport as NT

logLevelFormatter :: Message -> Process (Maybe String)
logLevelFormatter m = handleMessage m showLevel
  where
    showLevel :: LogLevel -> Process String
    showLevel = return . show

$(remotable ['logLevelFormatter])

logFormat :: Closure (Message -> Process (Maybe String))
logFormat = $(mkStaticClosure 'logLevelFormatter)

testLoggingProcess :: Process (ProcessId, TChan String)
testLoggingProcess = do
  chan <- liftIO $ newTChanIO
  let cleanup  = return ()
  let format   = return
  pid <- systemLog (writeLog chan) cleanup Debug format
  addFormatter pid logFormat
  sleep $ seconds 1
  return (pid, chan)
  where
    writeLog chan = liftIO . atomically . writeTChan chan

testLogLevels :: (Log.Logger logger, ToLog tL)
              => MVar ()
              -> logger
              -> LogLevel
              -> LogLevel
              -> (LogLevel -> tL)
              -> TestResult Bool
              -> Process ()
testLogLevels lck logger from to fn result = do
  void $ liftIO $ takeMVar lck
  infoPair <- testLoggingProcess
  let lvls = enumFromTo from to
  logIt logger fn lvls
  testHarness lvls infoPair result

  let pid = fst infoPair
  kill pid "finished"
  awaitExit pid
  liftIO $ putMVar lck ()
  where
    logIt _  _ []     = return ()
    logIt lc f (l:ls) = sendLog lc (f l) l >> logIt lc f ls

testHarness :: [LogLevel]
            -> (ProcessId, TChan String)
            -> TestResult Bool
            -> Process ()
testHarness []     (_, chan)   result = do
  liftIO (atomically (isEmptyTChan chan)) >>= stash result
testHarness levels p@(_, chan) result = do
  msg <- liftIO $ atomically $ readTChan chan
  -- liftIO $ putStrLn $ "testHarness handling " ++ msg
  let item = readEither msg
  case item of
    Right i -> testHarness (delete i levels) p result
    Left  _ -> testHarness levels            p result
  where
    readEither :: String -> Either String LogLevel
    readEither s =
      case [ x | (x,"") <- readPrec_to_S read' minPrec s ] of
        [x] -> Right x
        _   -> Left "read: ambiguous parse"

    read' =
      do x <- readPrec
         lift P.skipSpaces
         return x

tests :: NT.Transport  -> IO [Test]
tests transport = do
  let ch = logChannel
  localNode <- newLocalNode transport $ __remoteTable initRemoteTable
  lock <- newMVar ()
  return [
      testGroup "Log Reports / LogText"
        (map (mkTestCase lock ch simpleShowToLog localNode) (enumFromTo Debug Emergency))
    , testGroup "Logging Raw Messages"
        (map (mkTestCase lock ch messageToLog localNode) (enumFromTo Debug Emergency))
    , testGroup "Custom Formatters"
        (map (mkTestCase lock ch messageRaw localNode) (enumFromTo Debug Emergency))
    ]
  where
    mkTestCase lck ch' rdr ln lvl = do
      let l = show lvl
      testCase l (delayedAssertion ("Expected up to " ++ l)
                  ln True $ testLogLevels lck ch' Debug lvl rdr)

    simpleShowToLog = (LogText . show)
    messageToLog    = unsafeWrapMessage . show
    messageRaw      = unsafeWrapMessage

main :: IO ()
main = testMain $ tests

