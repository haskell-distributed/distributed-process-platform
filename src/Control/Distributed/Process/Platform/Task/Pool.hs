{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE UndecidableInstances       #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Platform.Task.Pool
-- Copyright   :  (c) Tim Watson 2013
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- A pool of resources. The resources are created when the pool starts, by
-- evaluating a /generator/ expression. Resources are acquired and released
-- by client interaction with the pool process. Once acquired, a resource
-- is /locked/ by the pool and will not be offered to future clients until
-- they're released.
--
-- If a client (process) crashes whilst the resource is locked, the pool will
-- either release the resource automatically, destroy it, or leave it
-- permanently locked - depending on the 'ReclamationStrategy' chosen during
-- pool initialisation. The pool also monitors resources - for this reason, only
-- resource types that implement the 'Resource' typeclass can be used - and
-- if they crash, will restart them on demand.
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Platform.Task.Pool where

import Control.Applicative (Applicative, (<$>))
import Control.Concurrent.STM.TBQueue
import Control.DeepSeq (NFData)
import Control.Distributed.Process
  ( Process
  , Closure
  , MonitorRef
  , ProcessMonitorNotification(..)
  , ProcessId
  , SendPort
  , liftIO
  , spawnLocal
  , handleMessageIf
  , send
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
  , unsafeWrapMessage
  )
-- import Control.Distributed.Process.Serializable hiding (SerializableDict)
import Control.Distributed.Process.Platform.Internal.IdentityPool
  ( IDPool
  , newIdPool
  , takeId
  )
import Control.Distributed.Process.Platform.Internal.Queue.SeqQ (SeqQ)
import qualified Control.Distributed.Process.Platform.Internal.Queue.SeqQ as Queue
import Control.Distributed.Process.Platform.Internal.Containers.MultiMap (MultiMap)
import qualified Control.Distributed.Process.Platform.Internal.Containers.MultiMap as MultiMap
import Control.Distributed.Process.Platform.Internal.Primitives
  ( spawnMonitorLocal
  , awaitExit
  )
import Control.Distributed.Process.Platform.Internal.Types
  ( Resolvable(..)
  , Shutdown(..)
  , Linkable(..)
  , NFSerializable(..)
  , ExitReason(..)
  )
import Control.Distributed.Process.Platform.ManagedProcess
  ( channelControlPort
  , handleControlChan
  , handleInfo
  , handleRaw
  , handleRpcChan
  , continue
  , defaultProcess
  , noReply_
  , InitHandler
  , InitResult(..)
  , ProcessAction
  , ProcessDefinition(..)
  , ControlChannel
  , ControlPort
  , UnhandledMessagePolicy(..)
  )
import qualified Control.Distributed.Process.Platform.ManagedProcess as MP
  ( serve
  )
import Control.Distributed.Process.Platform.ManagedProcess.Client
  ( callChan
  , cast
  )
import Control.Distributed.Process.Platform.Supervisor (SupervisorPid)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Serializable
import Control.Monad (void, forM_)
import Control.Monad.IO.Class (MonadIO)
import qualified Control.Monad.State as ST
  ( MonadState
  , MonadTrans
  , StateT
  , get
  , lift
  , modify
  , put
  , runStateT
  )
import Data.Accessor
  ( Accessor
  , accessor
  , (^:)
  , (^=)
  , (.>)
  , (^.)
  )
import Data.Binary
-- import Data.Binary (Binary(put, get))
import Data.Foldable (foldlM)
import Data.Hashable
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Sequence
  ( Seq
  , ViewR(..)
  , ViewL(..)
  , (<|)
  , (|>)
  , viewr
  , viewl
  )
import qualified Data.Sequence as Seq (empty, singleton, null, length)
import Data.Set (Set)
import Data.HashSet (HashSet)
import qualified Data.HashSet as HashSet
import qualified Data.Set as Set

import Data.Typeable (Typeable)
import GHC.Generics

--------------------------------------------------------------------------------
-- Client Handles                                                             --
--------------------------------------------------------------------------------

data ResourcePool r = ResourcePool { poolAddr :: !ProcessId }
  deriving (Typeable, Generic, Eq, Show)
instance (Serializable r) => Binary (ResourcePool r) where

instance Linkable (ResourcePool r) where
  linkTo = link . poolAddr

instance Resolvable (ResourcePool r) where
  resolve = return . Just . poolAddr

class (Serializable r, Hashable r, Eq r, Ord r) => Referenced r
instance (Serializable r, Hashable r, Eq r, Ord r) => Referenced r

--------------------------------------------------------------------------------
-- Command Messages / Client Facing Types                                     --
--------------------------------------------------------------------------------

data AcquireResource = AcquireResource ProcessId
  deriving (Typeable, Generic, Eq, Show)
instance Binary AcquireResource where
instance NFData AcquireResource where

data ReleaseResource r = ReleaseResource r
  deriving (Typeable, Generic)
instance (Serializable r) => Binary (ReleaseResource r) where
instance (NFData r) => NFData (ReleaseResource r) where

data ReclamationStrategy = Release | Destroy | PermLock

data PoolStatsInfo = PoolStatsInfo    String String  |
                     PoolStatsCounter String Integer |
                     PoolStatsFloat   String Double
  deriving (Typeable, Generic)
instance Binary PoolStatsInfo where

data PoolStats = PoolStats { totalCount    :: Integer
                           , activeCount   :: Integer
                           , inactiveCount :: Integer
                           , backendStats  :: [PoolStatsInfo]
                           } deriving (Typeable, Generic)
instance Binary PoolStats where

--------------------------------------------------------------------------------
-- Resource Types, Pool Backends and the @Pool@ monad.                        --
--------------------------------------------------------------------------------

data Status = Alive | Dead | Unknown

data Resource r =
  Resource { create   :: Process r
           , destroy  :: r -> Process ()
           , checkRef :: Message -> r -> Process (Maybe Status)
           , accept   :: (Ticket r) -> r -> Process ()
           }

data InitPolicy = OnDemand | OnInit deriving (Typeable, Eq, Show)

data ResourceQueue r = ResourceQueue { _available :: Seq r
                                     , _busy      :: Set r
                                     } deriving (Typeable)

-- We use 'RotationPolicy' to determine whether released resources are added
-- to the head or tail of the ResourceQueue. We always dequeue the 'head'
-- element (i.e., the left side of the sequence), allowing for both LRU and MRU.

data RotationPolicy s r = LRU
                        | MRU
                        | Custom (PoolState s r -> r -> PoolState s r)

data PoolState s r = PoolState { queue          :: ResourceQueue r
                               , initPolicy     :: InitPolicy
                               , rotationPolicy :: RotationPolicy s r
                               , internalState  :: s
                               , resourceType   :: Resource r
                               } deriving (Typeable)

initialPoolState :: forall s r. (Referenced r)
                 => Resource r
                 -> InitPolicy
                 -> RotationPolicy s r
                 -> s
                 -> PoolState s r
initialPoolState rt ip rp st =
  PoolState { queue          = ResourceQueue Seq.empty Set.empty
            , initPolicy     = ip
            , rotationPolicy = rp
            , internalState  = st
            , resourceType   = rt
            }

newtype Pool s r a = Pool { unPool :: ST.StateT (PoolState s r) Process a }
  deriving (Functor, Monad, ST.MonadState (PoolState s r)
           , MonadIO, Typeable, Applicative)

getState :: forall s r. Pool s r s
getState = ST.get >>= return . internalState

putState :: forall s r. s -> Pool s r ()
putState st = modifyPoolState $ \s -> s { internalState = st }

withState :: forall s r. (s -> s) -> Pool s r ()
withState fn =
  modifyPoolState $ \s -> s { internalState = (fn $ internalState s) }

-- | TODO MonadTrans instance? lift :: (Monad m) => m a -> t m a
lift :: forall s r a . Process a -> Pool s r a
lift p = Pool $ ST.lift p

foldResources :: forall s r a. (Referenced r)
              => (a -> r -> Pool s r a)
              -> a
              -> Pool s r a
foldResources = undefined

resourceQueueLen :: forall s r . (Referenced r) => Pool s r (Int, Int)
resourceQueueLen = do
  st <- getPoolState
  return $ lengths (st ^. resourceQueue)
  where
    lengths ResourceQueue{..} = (Seq.length _available, Set.size _busy)

addPooledResource :: forall s r. (Referenced r) => r -> Pool s r ()
addPooledResource = releasePooledResource

releasePooledResource :: forall s r . (Referenced r) => r -> Pool s r ()
releasePooledResource = modifyPoolState . doReleasePooledResource

acquirePooledResource :: forall s r. (Referenced r) => Pool s r (Maybe r)
acquirePooledResource = do
  st <- getPoolState
  let ac' = doAcquirePooledResource st
  case ac' of
    Nothing       -> return Nothing
    Just (r, st') -> do putPoolState st'
                        return $ Just r

removePooledResource :: forall s r. (Referenced r) => r -> Pool s r ()
removePooledResource r = do
  modifyPoolState (resourceQueue .> busy ^: Set.delete r)

getInitPolicy :: forall s r . Pool s r InitPolicy
getInitPolicy = ST.get >>= return . initPolicy

getRotationPolicy :: forall s r . Pool s r (RotationPolicy s r)
getRotationPolicy = ST.get >>= return . rotationPolicy

getResourceType :: forall s r . Pool s r (Resource r)
getResourceType = do
  st <- getPoolState
  return (resourceType st)

getPoolState :: forall s r. Pool s r (PoolState s r)
getPoolState = ST.get

putPoolState :: forall s r. PoolState s r -> Pool s r ()
putPoolState = ST.put

modifyPoolState :: forall s r. (PoolState s r -> PoolState s r) -> Pool s r ()
modifyPoolState = ST.modify

doAcquirePooledResource :: forall s r . (Referenced r)
                        => PoolState s r -> Maybe (r, PoolState s r)
doAcquirePooledResource st@PoolState{..} =
  case (viewr avail) of
    EmptyR    -> Nothing
    (q' :> a) -> Just (a, ( (resourceQueue .> available ^= q')
                          . (resourceQueue .> busy ^: Set.insert a) $ st ))
  where
    avail = queue ^. available

doReleasePooledResource :: forall s r . (Referenced r)
                        => r -> PoolState s r -> PoolState s r
doReleasePooledResource r st@PoolState{..}
  | (Custom fn) <- rotationPolicy = fn st r
  | otherwise {- LRU or MRU -}    =
    let avail = case rotationPolicy of
                  LRU -> (r <|)
                  MRU -> (|> r)
    in ( (resourceQueue .> available ^: avail)
       . (resourceQueue .> busy ^: Set.delete r) $ st )

data Take r = Block | Take r

data PoolBackend s r =
  PoolBackend { acquire  :: Pool s r (Take r)
              , release  :: r -> Pool s r ()
              , dispose  :: r -> Pool s r ()
              , setup    :: Pool s r ()
              , teardown :: ExitReason -> Pool s r ()
              , infoCall :: Message -> Pool s r ()
              , getStats :: Pool s r [PoolStatsInfo]
              }

--------------------------------------------------------------------------------
-- Server State                                                               --
--------------------------------------------------------------------------------

data Ticket r = Ticket { ticketOwner :: ProcessId
                       , ticketChan  :: SendPort r
                       } deriving (Typeable)

data State s r = State { _pool      :: PoolBackend s r
                       , _poolState :: PoolState s r
                       , _clients   :: MultiMap ProcessId r
                       , _pending   :: SeqQ (Ticket r)
                       , _locked    :: Set r
                       , reclaim    :: ReclamationStrategy
                       }

defaultState :: forall r s. (Referenced r)
             => Resource r
             -> PoolBackend s r
             -> InitPolicy
             -> RotationPolicy s r
             -> ReclamationStrategy
             -> s
             -> State s r
defaultState rt pl ip rp rs ps = State { _pool      = pl
                                       , _poolState = initialPoolState rt ip rp ps
                                       , _clients   = MultiMap.empty
                                       , _pending   = Queue.empty
                                       , reclaim    = rs
                                       }

--------------------------------------------------------------------------------
-- Client Facing API                                                          --
--------------------------------------------------------------------------------

transaction :: forall a r. (Referenced r)
        => ResourcePool r
        -> (r -> Process a)
        -> Process a
transaction pool proc = do
  acquireResource pool >>= \r -> proc r `finally` releaseResource pool r

checkOut :: forall r. (Referenced r) => ResourcePool r -> Process r
checkOut = acquireResource

acquireResource :: forall r. (Referenced r)
                => ResourcePool r
                -> Process r
acquireResource pool =
  getSelfPid >>= callChan pool . AcquireResource >>= receiveChan

checkIn :: forall r. (Referenced r) => ResourcePool r -> r -> Process ()
checkIn = releaseResource

releaseResource :: forall r. (Referenced r)
                => ResourcePool r
                -> r
                -> Process ()
releaseResource pool = cast pool . ReleaseResource

transfer :: forall r. (Referenced r)
         => ResourcePool r
         -> r
         -> ProcessId
         -> Process ()
transfer = undefined

--------------------------------------------------------------------------------
-- Starting/Running a Resource Pool                                           --
--------------------------------------------------------------------------------

-- | Run the resource pool server loop.
--
runPool :: forall s r. (Referenced r)
        => Resource r
        -> PoolBackend s r
        -> InitPolicy
        -> RotationPolicy s r
        -> ReclamationStrategy
        -> s
        -> Process ()
runPool t p i r d s =
  MP.serve (t, p, i, r, d, s) poolInit (processDefinition :: ProcessDefinition (State s r))

--------------------------------------------------------------------------------
-- Pool Initialisation/Startup                                                --
--------------------------------------------------------------------------------

poolInit :: forall r s. (Referenced r)
         => InitHandler (Resource r, PoolBackend s r,
                         InitPolicy, RotationPolicy s r,
                         ReclamationStrategy, s) (State s r)
poolInit (rt, pb, ip, rp, rs, ps) =
  let st  = defaultState rt pb ip rp rs ps
      ps' = st ^. poolState
  in do ((), pState) <- runPoolStateT ps' (setup pb)
        return $ InitOk ((poolState ^= pState) $ st) Infinity

runPoolStateT :: PoolState s r -> Pool s r a -> Process (a, PoolState s r)
runPoolStateT state proc = ST.runStateT (unPool proc) state

{-
      case lState of
        Block ls -> do lState' <- reset limiter ls
                       return $ ((limiterState ^= lState') $ st)
        Take  ls -> do let (wId, pool') = takeId _workerIds
                       ref <- create resourceType wId
                       startWorkers $ ( (workers ^: Map.insert ref wId)
                                      . (workerIds ^= pool')
                                      . (limiterState ^= ls)
                                      $ st
                                      )
-}

--------------------------------------------------------------------------------
-- Process Definition/State & API Handlers                                    --
--------------------------------------------------------------------------------

processDefinition :: forall s r.
                     (Referenced r)
                  => ProcessDefinition (State s r)
processDefinition =
  defaultProcess { apiHandlers     = [ handleRpcChan handleAcquire ]
                 , infoHandlers    = [ handleInfo handleMonitorSignal
                                     , handleRaw  handlePotentialRef ]
                 , shutdownHandler = handleShutdown
                 , unhandledMessagePolicy = Drop
                 }

handleAcquire :: forall s r. (Referenced r)
              => State s r
              -> SendPort r
              -> AcquireResource
              -> Process (ProcessAction (State s r))
handleAcquire st@State{..} clientPort (AcquireResource clientPid) = do
  void $ monitor clientPid
  (take, pst) <- runPoolStateT (st ^. poolState) (acquire (st ^. pool))
  let st' = (poolState ^= pst) st
  case take of
    Block  -> addPending clientPid clientPort st'
    Take r -> allocateResource r clientPid clientPort st'

handleMonitorSignal :: forall s r. (Referenced r)
                    => State s r
                    -> ProcessMonitorNotification
                    -> Process (ProcessAction (State s r))
handleMonitorSignal st mSig@(ProcessMonitorNotification _ pid _) = do
  let mp = MultiMap.delete pid (st ^. clients)
  case mp of
    Nothing        -> handoffToBackend mSig =<< clearTickets pid st
    Just (rs, mp') -> applyReclamationStrategy pid rs ((clients ^= mp') st)

handoffToBackend :: forall s r m. (Referenced r, Serializable m)
                 => m
                 -> State s r
                 -> Process (ProcessAction (State s r))
handoffToBackend msg st = backendInfoCall (unsafeWrapMessage msg) st

backendInfoCall :: forall s r . (Referenced r)
                => Message
                -> State s r
                -> Process (ProcessAction (State s r))
backendInfoCall msg st = do
  ((), pst) <- runPoolStateT ps $ infoCall pl msg
  continue $ (poolState ^= pst) st
  where
    ps = st ^. poolState
    pl = st ^. pool

clearTickets :: forall s r. (Referenced r)
             => ProcessId
             -> State s r
             -> Process (State s r)
clearTickets pid st@State{..} = do
  return $ (pending ^: Queue.filter ((/= pid) . ticketOwner)) st

applyReclamationStrategy :: forall s r . (Referenced r)
                         => ProcessId
                         -> HashSet r
                         -> State s r
                         -> Process (ProcessAction (State s r))
applyReclamationStrategy pid rs st@State{..} =
  let act = case reclaim of
              Release  -> doRelease
              Destroy  -> doDestroy
              PermLock -> doPermLock
  in act st rs >>= clearTickets pid >>= handoffToBackend pid
  where
    doRelease st' rs' = do
      ((), pst) <- runPoolStateT poolState' (forM_ (HashSet.toList rs') release')
      return $ ( (poolState ^= pst) st' )

    doDestroy st' rs' = do
      let rs'' = HashSet.toList rs'
      ((), pst) <- runPoolStateT poolState' (forM_ rs'' dispose')
      return $ ( (poolState ^= pst) st' )

    doPermLock st' rs' = do
      -- we mark resources as locked, but to what end? isn't it sufficient to
      -- just /ignore/ the client's death and leave those resources marked (in
      -- the pool backend) as @busy@ here instead? When do we ever check this
      -- /locked list/ anyway???
      return $ ( (locked ^: Set.union (Set.fromList (HashSet.toList rs'))) st' )

    poolState'    = st ^. poolState
    dispose'      = dispose (st ^. pool)
    release'      = release (st ^. pool)
    resourceType' = resourceType (st ^. poolState)

handlePotentialRef :: forall s r. (Referenced r)
                   => State s r
                   -> Message
                   -> Process (ProcessAction (State s r))
handlePotentialRef = undefined

addPending :: forall s r . (Referenced r)
           => ProcessId
           -> SendPort r
           -> State s r
           -> Process (ProcessAction (State s r))
addPending clientPid clientPort st = do
  let st' = ((pending ^: \q -> Queue.enqueue q $ Ticket clientPid clientPort) st)
  continue st'

allocateResource :: forall s r. (Referenced r)
                 => r
                 -> ProcessId
                 -> SendPort r
                 -> State s r
                 -> Process (ProcessAction (State s r))
allocateResource ref clientPid clientPort st = do
  unsafeSendChan clientPort ref
  continue $ ( (clients ^: MultiMap.insert clientPid ref) st )

handleShutdown :: forall s r . State s r -> ExitReason -> Process ()
handleShutdown st@State{..} reason =
  void $ runPoolStateT (st ^. poolState) (teardown (st ^. pool) $ reason)

--------------------------------------------------------------------------------
-- Accessors, State/Stats Management & Utilities                              --
--------------------------------------------------------------------------------

pool :: forall s r. Accessor (State s r) (PoolBackend s r)
pool = accessor _pool (\pl' st -> st { _pool = pl' })

poolState :: forall s r. Accessor (State s r) (PoolState s r)
poolState = accessor _poolState (\ps' st -> st { _poolState = ps' })

clients :: forall s r. Accessor (State s r) (MultiMap ProcessId r)
clients = accessor _clients (\cs' st -> st { _clients = cs' })

pending :: forall s r. Accessor (State s r) (SeqQ (Ticket r))
pending = accessor _pending (\tks' st -> st { _pending = tks' })

locked :: forall s r. Accessor (State s r) (Set r)
locked = accessor _locked (\lkd' st -> st { _locked = lkd' })

-- PoolState and ResourceQueue lenses

resourceQueue :: forall s r. Accessor (PoolState s r) (ResourceQueue r)
resourceQueue = accessor queue (\q' st -> st { queue = q' })

available :: forall r. Accessor (ResourceQueue r) (Seq r)
available = accessor _available (\q' st -> st { _available = q' })

busy :: forall r. Accessor (ResourceQueue r) (Set r)
busy = accessor _busy (\s' st -> st { _busy = s' })

