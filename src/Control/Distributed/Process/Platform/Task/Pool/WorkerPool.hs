{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE UndecidableInstances       #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Platform.Task.Pool.WorkerPool
-- Copyright   :  (c) Tim Watson 2013
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Platform.Task.Pool.WorkerPool where

import Control.DeepSeq (NFData)
import Control.Distributed.Process
  ( Process
  , MonitorRef
  , ProcessMonitorNotification(..)
  , ProcessId
  , SendPort
  , liftIO
  , spawnLocal
  , handleMessageIf
  , send
  , sendChan
  , unsafeSendChan
  , getSelfPid
  , receiveChan
  , exit
  , kill
  , match
  , Match
  , Message
  , link
  , monitor
  , unmonitor
  , finally
  )
import Control.Distributed.Process.Platform.Internal.IdentityPool
  ( IDPool
  , newIdPool
  , takeId
  )
import Control.Distributed.Process.Platform.Internal.Primitives
  ( spawnMonitorLocal
  , awaitExit
  )
import Control.Distributed.Process.Platform.Internal.Types
  ( Resolvable(..)
  , Shutdown(..)
  , Linkable(..)
  , NFSerializable(..)
  )
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Platform.Task.Pool
import Data.Binary
import Data.Hashable
import Data.Typeable (Typeable)
import GHC.Generics

type PoolSize        = Integer
type InitialPoolSize = Integer

newtype WorkerResource = Worker { impl :: Process () } deriving (Typeable)

-- TODO: deduplicate these two definitions

worker :: WorkerResource -> Resource (ProcessId, MonitorRef)
worker w =
  Resource {
      create   = spawnMonitorLocal (impl w)
    , destroy  = \(p, _) -> exit p Shutdown >> awaitExit p
    , checkRef = (\m (_, r) -> do
         handleMessageIf m (\(ProcessMonitorNotification r' _ _) -> r == r')
                           (\_ -> return Dead))
    , accept   = \t r -> sendChan (ticketChan t) r
    }

newtype SpawnResource =
  SpawnResource { spawnImpl :: Process (ProcessId, MonitorRef) }
  deriving (Typeable)

spawnWorker :: SpawnResource -> Resource (ProcessId, MonitorRef)
spawnWorker w =
  Resource {
      create   = spawnImpl w
    , destroy  = \(p, _) -> exit p Shutdown >> awaitExit p
    , checkRef = (\m (_, r) -> do
         handleMessageIf m (\(ProcessMonitorNotification r' _ _) -> r == r')
                           (\_ -> return Dead))
    , accept   = \t r -> sendChan (ticketChan t) r
    }

data WPState = WPState { sizeLimit :: PoolSize
--                       , acquired  :: Integer
--                       , released  :: Integer
                         -- TODO: keep track of things and report via getStats
                       }

runWorkerPool :: Resource (ProcessId, MonitorRef)
              -> PoolSize
              -> InitPolicy
              -> RotationPolicy WPState (ProcessId, MonitorRef)
              -> ReclamationStrategy
              -> Process ()
runWorkerPool rt sz ip rp rs = runPool rt poolDef ip rp rs (initState sz)

initState :: PoolSize -> WPState
initState sz = WPState sz

poolDef :: forall r. (Referenced r) => PoolBackend WPState r
poolDef = PoolBackend { acquire  = apiAcquire
                      , release  = apiRelease
                      , dispose  = apiDispose
                      , setup    = apiSetup
                      , teardown = const $ return () -- TODO: FIXME!
                      , infoCall = apiInfoCall
                      , getStats = apiGetStats
                      }

apiAcquire  :: forall r. (Referenced r) => Pool WPState r (Take r)
apiAcquire = do
  pol <- getInitPolicy
  case pol of
    OnDemand -> do sz <- getState >>= return . sizeLimit
                   (a, b) <- resourceQueueLen
                   if (toInteger $ a + b) < sz
                      then tryAcquire
                      else return Block
    OnInit   -> tryAcquire
  where
    tryAcquire = return . maybe Block Take =<< acquirePooledResource

apiRelease :: forall r. (Referenced r) => r -> Pool WPState r ()
apiRelease = undefined

apiDispose :: forall r. (Referenced r) => r -> Pool WPState r ()
apiDispose r = do
  rType <- getResourceType
  res <- lift $ destroy rType r
  removePooledResource r

apiSetup :: forall r. (Referenced r) => Pool WPState r ()
apiSetup = do
  pol <- getInitPolicy
  case pol of
    OnDemand -> return ()
    OnInit   -> startResources 0
  where
    startResources :: PoolSize -> Pool WPState r ()
    startResources cnt = do
      st <- getState :: Pool WPState r WPState
      if cnt <= (sizeLimit st)
         then do rType <- getResourceType
                 res <- lift $ create rType
                 addPooledResource res
                 startResources (cnt + 1)
         else return ()

apiInfoCall :: forall r. (Referenced r) => Message -> Pool WPState r ()
apiInfoCall = const $ return ()

apiGetStats :: forall r. (Referenced r) => Pool WPState r [PoolStatsInfo]
apiGetStats = do
  st <- getState
  return [PoolStatsCounter "sizeLimit" $ sizeLimit st]

