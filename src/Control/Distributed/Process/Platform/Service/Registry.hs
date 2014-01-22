{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# LANGUAGE MultiParamTypeClasses      #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Platform.Service.Registry
-- Copyright   :  (c) Tim Watson 2012 - 2013
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- The module provides an extended process registry, offering slightly altered
-- semantics to the built in @register@ and @unregister@ primitives and a richer
-- set of features:
--
-- * Associate (unique) keys with a process /or/ (unique key per-process) values
-- * Use any 'Keyable' algebraic data type as keys
-- * Query for process with matching keys / values / properties
-- * Atomically /give away/ names
-- * Forceibly re-allocate names to/from a third party
--
-- [Subscribing To Registry Events]
--
-- It is possible to monitor a registry for changes and be informed whenever
-- changes take place. All subscriptions are /key based/, which means that
-- you can subscribe to name or property changes for any process, so that any
-- property changes matching the key you've subscribed to will trigger a
-- notification (i.e., regardless of the process to which the property belongs).
--
-- The different types of event are defined by the 'KeyUpdateEvent' type.
--
-- Processes subscribe to registry events using @monitorName@ or its counterpart
-- @monitorProperty@. If the operation succeeds, this will evaluate to an
-- opaque /reference/ that can be used when subsequently handling incoming
-- notifications, which will be delivered to the subscriber's mailbox as
-- @RegistryKeyMonitorNotification keyIdentity opaqueRef event@, where @event@
-- has the type 'KeyUpdateEvent'.
--
-- Subscribers can filter the types of event they receive by using the lower
-- level @monitor@ function (defined in /this/ module - not the one defined
-- in distributed-process' @Primitives@) and passing a list of filtering
-- 'KeyUpdateEventMask'. Without these filters in place, a monitor event will
-- be fired for /every/ pertinent change.
--
-----------------------------------------------------------------------------
module Control.Distributed.Process.Platform.Service.Registry
  ( -- * Registry Keys
    KeyType(..)
  , Key(..)
  , Keyable
    -- * Defining / Starting A Registry
  , Registry(..)
  , start
  , run
    -- * Registration / Unregistration
  , addName
  , addProperty
  , registerName
  , registerValue
  , giveAwayName
  , RegisterKeyReply(..)
  , unregisterName
  , UnregisterKeyReply(..)
    -- * Queries / Lookups
  , lookupName
  , lookupProperty
  , registeredNames
  , foldNames
  , SearchHandle()
  , member
  , queryNames
  , findByProperty
  , findByPropertyValue
    -- * Monitoring / Waiting
  , monitor
  , monitorName
  , monitorProp
  , unmonitor
  , await
  , awaitTimeout
  , AwaitResult(..)
  , KeyUpdateEventMask(..)
  , KeyUpdateEvent(..)
  , RegKeyMonitorRef
  , RegistryKeyMonitorNotification(RegistryKeyMonitorNotification)
  ) where

{- DESIGN NOTES
This registry is a single process, parameterised by the types of key and
property value it can manage. It is, of course, possible to start multiple
registries and inter-connect them via registration, with one another.

The /Service/ API is intended to be a declarative layer in which you define
the managed processes that make up your services, and each /Service Component/
is registered and supervised appropriately for you, with the correct restart
strategies and start order calculated and so on. The registry is not only a
service locator, but provides the /wait for these dependencies to start first/
bit of the puzzle.

At some point, we'd like to offer a shared memory based registry, created on
behalf of a particular subsystem (i.e., some service or service group) and
passed implicitly using a reader monad or some such. This would allow multiple
processes to interact with the registry using STM (or perhaps a simple RWLock)
and could facilitate reduced contention.

Even for the singleton-process based registry (i.e., this one) we /might/ also
be better off separating the monitoring (or at least the notifications) from
the registration/mapping parts into separate processes.
-}

import Control.Exception (SomeException)
import Control.Distributed.Process hiding (call, monitor, unmonitor, mask)
import qualified Control.Distributed.Process.UnsafePrimitives as Unsafe (send)
import qualified Control.Distributed.Process as P (monitor)
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Platform.Internal.Primitives hiding (monitor)
import Control.Distributed.Process.Platform.Internal.Types
  ( Resolvable(..)
  , Routable(..)
  , ExitReason
  )
import qualified Control.Distributed.Process.Platform.Internal.Primitives as PL
  ( monitor
  )
import Control.Distributed.Process.Platform.ManagedProcess
  ( call
  , safeCall
  , UnhandledMessagePolicy(..)
  , cast
  , handleInfo
  , handleExit
  , reply
  , continue
  , input
  , defaultProcess
  , prioritised
  , InitHandler
  , InitResult(..)
  , ProcessAction
  , ProcessReply
  , ProcessDefinition(..)
  , PrioritisedProcessDefinition(..)
  , DispatchPriority
  , CallRef
  )
import qualified Control.Distributed.Process.Platform.ManagedProcess as MP
  ( pserve
  )
import Control.Distributed.Process.Platform.ManagedProcess.Server
  ( handleCallIf
  , handleCallFrom
  , handleCallFromIf
  , handleCast
  )
import Control.Distributed.Process.Platform.ManagedProcess.Server.Priority
  ( prioritiseInfo_
  , setPriority
  )
import Control.Distributed.Process.Platform.ManagedProcess.Server.Restricted
  ( RestrictedProcess
  , Result
  , getState
  )
import qualified Control.Distributed.Process.Platform.ManagedProcess.Server.Restricted as Restricted
  ( handleCall
  , reply
  )
import Control.Distributed.Process.Platform.Time
import Control.Monad (forM_, void)
import Data.Accessor
  ( Accessor
  , accessor
  , (^:)
  , (^=)
  , (^.)
  )
import Data.Binary
import Data.Foldable (Foldable)
import qualified Data.Foldable as Foldable
import Data.Maybe (fromJust, isJust)
import Data.Hashable
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as Map
import Control.Distributed.Process.Platform.Internal.Containers.MultiMap (MultiMap, Insertable)
import qualified Control.Distributed.Process.Platform.Internal.Containers.MultiMap as MultiMap
import Data.HashSet (HashSet)
import qualified Data.HashSet as Set
import Data.Typeable (Typeable)

import GHC.Generics

--------------------------------------------------------------------------------
-- Types                                                                      --
--------------------------------------------------------------------------------

-- | Describes how a key will be used - for storing names or properties.
data KeyType =
    KeyTypeAlias    -- ^ the key will refer to a name (i.e., named process)
  | KeyTypeProperty -- ^ the key will refer to a (per-process) property
  deriving (Typeable, Generic, Show, Eq)
instance Binary KeyType where
instance Hashable KeyType where

-- | A registered key. Keys can be mapped to names or (process-local) properties
-- in the registry. The 'keyIdentity' holds the key's value (e.g., a string or
-- similar simple data type, which must provide a 'Keyable' instance), whilst
-- the 'keyType' and 'keyScope' describe the key's intended use and ownership.
data Key a =
    Key
    { keyIdentity :: !a
    , keyType     :: !KeyType
    , keyScope    :: !(Maybe ProcessId)
    }
  deriving (Typeable, Generic, Show, Eq)
instance (Serializable a) => Binary (Key a) where
instance (Hashable a) => Hashable (Key a) where

-- | The 'Keyable' class describes types that can be used as registry keys.
-- The constraints ensure that the key can be stored and compared appropriately.
class (Show a, Eq a, Hashable a, Serializable a) => Keyable a
instance (Show a, Eq a, Hashable a, Serializable a) => Keyable a

-- | A phantom type, used to parameterise registry startup
-- with the required key and value types.
data Registry k v = Registry

-- Internal/Private Request/Response Types

data LookupKeyReq k = LookupKeyReq !(Key k)
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (LookupKeyReq k) where

data LookupPropReq k = PropReq (Key k)
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (LookupPropReq k) where

data LookupPropReply =
    PropFound !Message
  | PropNotFound
  deriving (Typeable, Generic)
instance Binary LookupPropReply where

data InvalidPropertyType = InvalidPropertyType
  deriving (Typeable, Generic, Show, Eq)
instance Binary InvalidPropertyType where

data RegNamesReq = RegNamesReq !ProcessId
  deriving (Typeable, Generic)
instance Binary RegNamesReq where

data UnregisterKeyReq k = UnregisterKeyReq !(Key k)
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (UnregisterKeyReq k) where

-- | The result of an un-registration attempt.
data UnregisterKeyReply =
    UnregisterOk -- ^ The given key was successfully unregistered
  | UnregisterInvalidKey -- ^ The given key was invalid and could not be unregistered
  | UnregisterKeyNotFound -- ^ The given key was not found (i.e., was not registered)
  deriving (Typeable, Generic, Eq, Show)
instance Binary UnregisterKeyReply where

-- Types used in (setting up and interacting with) key monitors

-- | Used to describe a subset of monitoring events to listen for.
data KeyUpdateEventMask =
    OnKeyRegistered      -- ^ receive an event when a key is registered
  | OnKeyUnregistered    -- ^ receive an event when a key is unregistered
  | OnKeyOwnershipChange -- ^ receive an event when a key's owner changes
  | OnKeyLeaseExpiry     -- ^ receive an event when a key's lease expires
  deriving (Typeable, Generic, Eq, Show)
instance Binary KeyUpdateEventMask where
instance Hashable KeyUpdateEventMask where

-- | An opaque reference used for matching monitoring events. See
-- 'RegistryKeyMonitorNotification' for more details.
newtype RegKeyMonitorRef =
  RegKeyMonitorRef { unRef :: (ProcessId, Integer) }
  deriving (Typeable, Generic, Eq, Show)
instance Binary RegKeyMonitorRef where
instance Hashable RegKeyMonitorRef where

instance Resolvable RegKeyMonitorRef where
  resolve = return . Just . fst . unRef

-- | Provides information about a key monitoring event.
data KeyUpdateEvent =
    KeyRegistered
    {
      owner :: !ProcessId
    }
  | KeyUnregistered
  | KeyLeaseExpired
  | KeyOwnerDied
    {
      diedReason :: !DiedReason
    }
  | KeyOwnerChanged
    {
      previousOwner :: !ProcessId
    , newOwner      :: !ProcessId
    }
  deriving (Typeable, Generic, Eq, Show)
instance Binary KeyUpdateEvent where

reason :: KeyUpdateEvent -> DiedReason
reason (KeyOwnerDied dr) = dr
reason _                 = DiedNormal

-- | This message is delivered to processes which are monioring a
-- registry key. The opaque monitor reference will match (i.e., be equal
-- to) the reference returned from the @monitor@ function, which the
-- 'KeyUpdateEvent' describes the change that took place.
data RegistryKeyMonitorNotification k =
  RegistryKeyMonitorNotification !k !RegKeyMonitorRef !KeyUpdateEvent !ProcessId
  deriving (Typeable, Generic)
instance (Keyable k) => Binary (RegistryKeyMonitorNotification k) where
deriving instance (Keyable k) => Eq (RegistryKeyMonitorNotification k)
deriving instance (Keyable k) => Show (RegistryKeyMonitorNotification k)

instance (Keyable k, Addressable a) =>
         Observable (a, Key k)
                    (a, RegKeyMonitorRef)
                    (RegistryKeyMonitorNotification k) where
  observe   (a, k) = monitor a k Nothing >>= \r -> return (a, r)
  unobserve (a, r) = error "unmonitor a r"  -- TODO: FIXME!
  observableFrom (a, r) (RegistryKeyMonitorNotification _ r' e s) = do
    pid <- resolve a
    maybe (return Nothing)
          (\p ->
            case p == s && r == r' of
              True  -> return $ Just $ reason e
              False -> return Nothing) pid

data RegisterKeyReq k = RegisterKeyReq !(Key k)
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (RegisterKeyReq k) where

-- | The (return) value of an attempted registration.
data RegisterKeyReply =
    RegisteredOk      -- ^ The given key was registered successfully
  | AlreadyRegistered -- ^ The key was already registered
  deriving (Typeable, Generic, Eq, Show)
instance Binary RegisterKeyReply where

-- | A cast message used to atomically give a name/key away to another process.
data GiveAwayName k = GiveAwayName !ProcessId !(Key k)
  deriving (Typeable, Generic)
instance (Keyable k) => Binary (GiveAwayName k) where
deriving instance (Keyable k) => Eq (GiveAwayName k)
deriving instance (Keyable k) => Show (GiveAwayName k)

data MonitorReq k = MonitorReq !(Key k) !(Maybe [KeyUpdateEventMask])
  deriving (Typeable, Generic)
instance (Keyable k) => Binary (MonitorReq k) where

data UnmonitorReq = UnmonitorReq !RegKeyMonitorRef
  deriving (Typeable, Generic)
instance Binary UnmonitorReq where

-- | The result of an @await@ operation.
data AwaitResult k =
    RegisteredName     !ProcessId !k   -- ^ The name was registered
  | ServerUnreachable  !DiedReason     -- ^ The server was unreachable (or died)
  | AwaitTimeout                       -- ^ The operation timed out
  deriving (Typeable, Generic, Eq, Show)
instance (Keyable k) => Binary (AwaitResult k) where

-- Server state

-- On the server, a monitor reference consists of the actual
-- RegKeyMonitorRef which we can 'sendTo' /and/ the which
-- the client matches on, plus an optional list of event masks
data KMRef = KMRef { ref  :: !RegKeyMonitorRef
                   , mask :: !(Maybe [KeyUpdateEventMask])
                     -- use Nothing to monitor every event
                   }
  deriving (Typeable, Generic, Show)
instance Hashable KMRef where
-- instance Binary KMRef where
instance Eq KMRef where
  (KMRef a _) == (KMRef b _) = a == b

data State k v =
  State
  {
    _names          :: !(HashMap k ProcessId)
  , _properties     :: !(HashMap (ProcessId, k) v)
  , _monitors       :: !(MultiMap k KMRef)     -- TODO: SHIT - only 1 monitor per key!?????
  , _registeredPids :: !(HashSet ProcessId)
  , _listeningPids  :: !(HashSet ProcessId)
  , _monitorIdCount :: !Integer
  , _registryType   :: !(Registry k v)
  }
  deriving (Typeable, Generic)

-- Types used in \direct/ queries

-- TODO: enforce QueryDirect's usage over only local channels

data QueryDirect = QueryDirectNames | QueryDirectProperties | QueryDirectValues
  deriving (Typeable, Generic)
instance Binary QueryDirect where

-- NB: SHashMap is basically a shim, allowing us to copy a
-- pointer to our HashMap directly to the querying process'
-- mailbox with no serialisation or even deepseq evaluation
-- required. We disallow remote queries (i.e., from other nodes)
-- and thus the Binary instance below is never used (though it's
-- required by the type system) and will in fact generate errors if
-- you attempt to use it at runtime.
data SHashMap k v = SHashMap [(k, v)] (HashMap k v)
  deriving (Typeable, Generic)

instance (Keyable k, Serializable v) =>
         Binary (SHashMap k v) where
  put = error "AttemptedToUseBinaryShim"
  get = error "AttemptedToUseBinaryShim"
{- a real instance could look something like this:

  put (SHashMap _ hmap) = put (toList hmap)
  get = do
    hm <- get :: Get [(k, v)]
    return $ SHashMap [] (fromList hm)
-}

newtype SearchHandle k v = RS { getRS :: HashMap k v }
  deriving (Typeable)

instance (Keyable k) => Functor (SearchHandle k) where
  fmap f (RS m) = RS $ Map.map f m
instance (Keyable k) => Foldable (SearchHandle k) where
  foldr f acc = Foldable.foldr f acc . getRS
-- TODO: add Functor and Traversable instances

--------------------------------------------------------------------------------
-- Starting / Running A Registry                                              --
--------------------------------------------------------------------------------

start :: forall k v. (Keyable k, Serializable v)
      => Registry k v
      -> Process ProcessId
start reg = spawnLocal $ run reg

run :: forall k v. (Keyable k, Serializable v)
    => Registry k v
    -> Process ()
run reg = MP.pserve () (initIt reg) serverDefinition

initIt :: forall k v. (Keyable k, Serializable v)
       => Registry k v
       -> InitHandler () (State k v)
initIt reg () = return $ InitOk initState Infinity
  where
    initState = State { _names          = Map.empty
                      , _properties     = Map.empty
                      , _monitors       = MultiMap.empty
                      , _registeredPids = Set.empty
                      , _listeningPids  = Set.empty
                      , _monitorIdCount = (1 :: Integer)
                      , _registryType   = reg
                      } :: State k v

--------------------------------------------------------------------------------
-- Client Facing API                                                          --
--------------------------------------------------------------------------------

-- | Sends a message to the process, or processes, corresponding to @key@.
-- If Key belongs to a unique object (name or aggregated counter), this
-- function will send a message to the corresponding process, or fail if there
-- is no such process. If Key is for a non-unique object type (counter or
-- property), Msg will be send to all processes that have such an object.
--
dispatch svr ky msg = undefined
  -- TODO: do a local-lookup and then sendTo the target

-- | Associate the calling process with the given (unique) key.
addName :: (Addressable a, Keyable k) => a -> k -> Process RegisterKeyReply
addName s n = getSelfPid >>= registerName s n

-- | Atomically transfer a (registered) name to another process. Has no effect
-- if the name does is not registered to the calling process!
--
giveAwayName :: (Addressable a, Keyable k) => a -> k -> ProcessId -> Process ()
giveAwayName s n p = do
  us <- getSelfPid
  cast s $ GiveAwayName p $ Key n KeyTypeAlias (Just us)

-- | Associate the given (non-unique) property with the current process.
-- If the property already exists, it will be overwritten with the new value.
addProperty :: (Addressable a, Keyable k, Serializable v)
            => a -> k -> v -> Process RegisterKeyReply
addProperty s k v = do
  call s $ (RegisterKeyReq (Key k KeyTypeProperty $ Nothing), v)

-- | Register the item at the given address.
registerName :: (Addressable a, Keyable k)
             => a -> k -> ProcessId -> Process RegisterKeyReply
registerName s n p = call s $ RegisterKeyReq (Key n KeyTypeAlias $ Just p)

-- | Register an item at the given address and associate it with a value.
-- If the property already exists, it will be overwritten with the new value.
registerValue :: (Addressable a, Addressable b, Keyable k, Serializable v)
              => a -> b -> k -> v -> Process RegisterKeyReply
registerValue s t n v = do
  Just p <- resolve t
  call s $ (RegisterKeyReq (Key n KeyTypeProperty $ Just p), v)

-- | Un-register a (unique) name for the calling process.
unregisterName :: (Addressable a, Keyable k)
               => a
               -> k
               -> Process UnregisterKeyReply
unregisterName s n = do
  self <- getSelfPid
  call s $ UnregisterKeyReq (Key n KeyTypeAlias $ Just self)

-- | Lookup the process identified by the supplied key. Evaluates to
-- @Nothing@ if the key is not registered.
lookupName :: (Addressable a, Keyable k) => a -> k -> Process (Maybe ProcessId)
lookupName s n = call s $ LookupKeyReq (Key n KeyTypeAlias Nothing)

-- | Lookup the value of a named property for the calling process. Evaluates to
-- @Nothing@ if the property (key) is not registered. If the assignment to a
-- value of type @v@ does not correspond to the type of properties stored by
-- the registry, the calling process will exit with the reason set to
-- @InvalidPropertyType@.
lookupProperty :: (Addressable a, Keyable k, Serializable v)
               => a
               -> k
               -> Process (Maybe v)
lookupProperty s n = do
  us <- getSelfPid
  res <- call s $ PropReq (Key n KeyTypeProperty (Just us))
  case res of
    PropNotFound  -> return Nothing
    PropFound msg -> do
      val <- unwrapMessage msg
      if (isJust val)
        then return val
        else die InvalidPropertyType

-- | Obtain a list of all registered keys.
registeredNames :: (Addressable a, Keyable k) => a -> ProcessId -> Process [k]
registeredNames s p = call s $ RegNamesReq p

-- | Monitor changes to the supplied name.
--
monitorName :: (Addressable a, Keyable k)
            => a -> k -> Process RegKeyMonitorRef
monitorName svr name = do
  let key' = Key { keyIdentity = name
                 , keyScope    = Nothing
                 , keyType     = KeyTypeAlias
                 }
  monitor svr key' Nothing

-- | Monitor changes to the supplied (property) key.
--
monitorProp :: (Addressable a, Keyable k)
            => a -> k -> ProcessId -> Process RegKeyMonitorRef
monitorProp svr key pid = do
  let key' = Key { keyIdentity = key
                 , keyScope    = Just pid
                 , keyType     = KeyTypeProperty
                 }
  monitor svr key' Nothing

-- | Low level monitor operation. For the given key, set up a monitor
-- filtered by any 'KeyUpdateEventMask' entries that are supplied.
monitor :: (Addressable a, Keyable k)
        => a
        -> Key k
        -> Maybe [KeyUpdateEventMask]
        -> Process RegKeyMonitorRef
monitor svr key' mask' = call svr $ MonitorReq key' mask'

-- | Remove a previously set monitor.
--
unmonitor :: (Addressable a)
          => a
          -> RegKeyMonitorRef
          -> Process ()
unmonitor s = call s . UnmonitorReq

-- | Await registration of a given key. This function will subsequently
-- block the evaluating process until the key is registered and a registration
-- event is dispatched to the caller's mailbox.
--
await :: (Addressable a, Keyable k)
      => a
      -> k
      -> Process (AwaitResult k)
await a k = awaitTimeout a Infinity k

-- | Await registration of a given key, but give up and return @AwaitTimeout@
-- if registration does not take place within the specified time period (@delay@).
awaitTimeout :: (Addressable a, Keyable k)
             => a
             -> Delay
             -> k
             -> Process (AwaitResult k)
awaitTimeout a d k = do
    p <- forceResolve a
    Just mRef <- PL.monitor p
    kRef <- monitor a (Key k KeyTypeAlias Nothing) (Just [OnKeyRegistered])
    let matches' = matches mRef kRef k
    let recv = case d of
                 Infinity -> receiveWait matches' >>= return . Just
                 Delay t  -> receiveTimeout (asTimeout t) matches'
    recv >>= return . maybe AwaitTimeout id
  where
    forceResolve addr = do
      mPid <- resolve addr
      case mPid of
        Nothing -> die "InvalidAddressable"
        Just p  -> return p

    matches mr kr k' = [
        matchIf (\(RegistryKeyMonitorNotification mk' kRef' ev' _) ->
                      (matchEv ev' && kRef' == kr && mk' == k'))
                (\(RegistryKeyMonitorNotification _ _ (KeyRegistered pid) _) ->
                  return $ RegisteredName pid k')
      , matchIf (\(ProcessMonitorNotification mRef' _ _) -> mRef' == mr)
                (\(ProcessMonitorNotification _ _ dr) ->
                  return $ ServerUnreachable dr)
      ]

    matchEv ev' = case ev' of
                    KeyRegistered _ -> True
                    _               -> False

-- Local (non-serialised) shared data access. See note [sharing] below.

findByProperty :: Keyable k
               => ProcessId
               -> k
               -> Process [ProcessId]
findByProperty pid key = do
    self <- getSelfPid
    withMonitor pid $ do
      cast pid $ (self, QueryDirectProperties)
      answer <- receiveWait [
          match (\(SHashMap _ m :: SHashMap ProcessId [k]) -> return $ Just m)
        , matchIf (\(ProcessMonitorNotification _ p _) -> p == pid)
                  (\_ -> return Nothing)
        , matchAny (\_ -> return Nothing)
        ]
      case answer of
        Nothing -> die "DisconnectedFromServer"
        Just m  -> return $ Map.foldlWithKey' matchKey [] m
  where
    matchKey ps p ks
      | key `elem` ks = p:ps
      | otherwise     = ps

findByPropertyValue :: (Keyable k, Serializable v, Eq v)
                    => ProcessId
                    -> k
                    -> v
                    -> Process [ProcessId]
findByPropertyValue pid key val = do
    self <- getSelfPid
    withMonitor pid $ do
      cast pid $ (self, QueryDirectValues)
      answer <- receiveWait [
          match (\(SHashMap _ m :: SHashMap ProcessId [(k, v)]) -> return $ Just m)
        , matchIf (\(ProcessMonitorNotification _ p _) -> p == pid)
                  (\_ -> return Nothing)
        , matchAny (\_ -> return Nothing) -- TODO: logging?
        ]
      case answer of
        Nothing -> die "DisconnectedFromServer"
        Just m  -> return $ Map.foldlWithKey' matchKey [] m
  where
    matchKey ps p ks
      | (key, val) `elem` ks = p:ps
      | otherwise            = ps

-- TODO: move to UnsafePrimitives over a passed {Send|Receive}Port here, and
-- avoid interfering with the caller's mailbox.

-- | Monadic left fold over all registered names/keys. The fold takes place
-- in the evaluating process.
foldNames :: forall b k. Keyable k
          => ProcessId
          -> b
          -> (b -> (k, ProcessId) -> Process b)
          -> Process b
foldNames pid acc fn = do
  self <- getSelfPid
  -- TODO: monitor @pid@ and die if necessary!!!
  cast pid $ (self, QueryDirectNames)
  -- Although we incur the cost of scanning our mailbox here (which we could
  -- avoid by spawning an intermediary perhaps), the message is delivered to
  -- us without any copying or serialisation overheads.
  SHashMap _ m <- expect :: Process (SHashMap k ProcessId)
  Foldable.foldlM fn acc (Map.toList m)

-- | Evaluate a query on a 'SearchHandle', in the calling process.
queryNames :: forall k b . Keyable k
       => ProcessId
       -> (SearchHandle k ProcessId -> Process b)
       -> Process b
queryNames pid fn = do
  self <- getSelfPid
  cast pid $ (self, QueryDirectNames)
  SHashMap _ m <- expect :: Process (SHashMap k ProcessId)
  fn (RS m)

-- | Tests whether or not the supplied key is registered, evaluated in the
-- calling process.
member :: (Keyable k, Serializable v)
       => k
       -> SearchHandle k v
       -> Bool
member k = Map.member k . getRS

-- note [sharing]:
-- We use the base library's UnsafePrimitives for these fold/query operations,
-- to pass a pointer to our internal HashMaps for read-only operations. There
-- is a potential cost to the caller, if their mailbox is full - we should move
-- to use unsafe channel's for this at some point.
--

--------------------------------------------------------------------------------
-- Server Process                                                             --
--------------------------------------------------------------------------------

serverDefinition :: forall k v. (Keyable k, Serializable v)
                 => PrioritisedProcessDefinition (State k v)
serverDefinition = prioritised processDefinition regPriorities
  where
    regPriorities :: [DispatchPriority (State k v)]
    regPriorities = [
        prioritiseInfo_ (\(ProcessMonitorNotification _ _ _) -> setPriority 100)
      ]

processDefinition :: forall k v. (Keyable k, Serializable v)
                  => ProcessDefinition (State k v)
processDefinition =
  defaultProcess
  {
    apiHandlers =
       [
         handleCallIf
              (input ((\(RegisterKeyReq (Key{..} :: Key k)) ->
                        keyType == KeyTypeAlias && (isJust keyScope))))
              handleRegisterName
       , handleCallIf
              (input ((\((RegisterKeyReq (Key{..} :: Key k)), _ :: v) ->
                        keyType == KeyTypeProperty && (isJust keyScope))))
              handleRegisterProperty
       , handleCallFromIf
              (input ((\((RegisterKeyReq (Key{..} :: Key k)), _ :: v) ->
                        keyType == KeyTypeProperty && (not $ isJust keyScope))))
              handleRegisterPropertyCR
         -- TODO: Better handling of RegisterKeyReq when k (or v) is ill-typed
       , handleCast handleGiveAwayName
       , handleCallIf
              (input ((\(LookupKeyReq (Key{..} :: Key k)) ->
                        keyType == KeyTypeAlias)))
              (\state (LookupKeyReq key') -> reply (findName key' state) state)
       , handleCallIf
              (input ((\(PropReq (Key{..} :: Key k)) ->
                        keyType == KeyTypeProperty && (isJust keyScope))))
              handleLookupProperty
       , handleCallIf
              (input ((\(UnregisterKeyReq (Key{..} :: Key k)) ->
                        keyType == KeyTypeAlias && (isJust keyScope))))
              handleUnregisterName
       , handleCallFrom handleMonitorReq
       , handleCallFrom handleUnmonitorReq
       , Restricted.handleCall handleRegNamesLookup
       , handleCast handleQuery
       ]
  , infoHandlers = [handleInfo handleMonitorSignal]
--  , exitHandlers = [handleExit $ \s (_ :: ExitReason) _ -> (liftIO $ putStrLn "Exit!") >> continue s]
--  , unhandledMessagePolicy = Drop
  } :: ProcessDefinition (State k v)

handleQuery :: forall k v. (Keyable k, Serializable v)
            => State k v
            -> (ProcessId, QueryDirect)
            -> Process (ProcessAction (State k v))
handleQuery st@State{..} (pid, qd) = do
    case qd of
      QueryDirectNames      -> Unsafe.send pid shmNames
      QueryDirectProperties -> Unsafe.send pid shmProps
      QueryDirectValues     -> Unsafe.send pid shmVals
    continue st
  where
    shmNames = SHashMap [] $ st ^. names
    shmProps = SHashMap [] xfmProps
    shmVals  = SHashMap [] xfmVals

    -- since we currently have to fold over our properties in order
    -- answer remote queries, the sharing we do here seems a bit pointless,
    -- however we'll be moving to a shared memory based registry soon

    xfmProps = Map.foldlWithKey' convProps Map.empty (st ^. properties)
    xfmVals  = Map.foldlWithKey' convVals Map.empty (st ^. properties)

    convProps m (p, k) _ =
      case Map.lookup p m of
        Nothing -> Map.insert p [k] m
        Just ks -> Map.insert p (k:ks) m

    convVals m (p, k) v =
      case Map.lookup p m of
        Nothing -> Map.insert p [(k, v)] m
        Just ks -> Map.insert p ((k, v):ks) m

handleRegisterName :: forall k v. (Keyable k, Serializable v)
                   => State k v
                   -> RegisterKeyReq k
                   -> Process (ProcessReply RegisterKeyReply (State k v))
handleRegisterName state (RegisterKeyReq Key{..}) = do
  let found = Map.lookup keyIdentity (state ^. names)
  case found of
    Nothing -> do
      let pid  = fromJust keyScope
      let refs = state ^. registeredPids
      refs' <- ensureMonitored pid refs
      notifySubscribers keyIdentity state (KeyRegistered pid)
      reply RegisteredOk $ ( (names ^: Map.insert keyIdentity pid)
                           . (registeredPids ^= refs')
                           $ state)
    Just pid ->
      if (pid == (fromJust keyScope))
         then reply RegisteredOk      state
         else reply AlreadyRegistered state

handleRegisterPropertyCR :: forall k v. (Keyable k, Serializable v)
                         => State k v
                         -> CallRef (RegisterKeyReply)
                         -> (RegisterKeyReq k, v)
                         -> Process (ProcessReply RegisterKeyReply (State k v))
handleRegisterPropertyCR st cr req = do
  pid <- resolve cr
  doRegisterProperty (fromJust pid) st req

handleRegisterProperty :: forall k v. (Keyable k, Serializable v)
                       => State k v
                       -> (RegisterKeyReq k, v)
                       -> Process (ProcessReply RegisterKeyReply (State k v))
handleRegisterProperty state req@((RegisterKeyReq Key{..}), _) = do
  doRegisterProperty (fromJust keyScope) state req

doRegisterProperty :: forall k v. (Keyable k, Serializable v)
                       => ProcessId
                       -> State k v
                       -> (RegisterKeyReq k, v)
                       -> Process (ProcessReply RegisterKeyReply (State k v))
doRegisterProperty scope state ((RegisterKeyReq Key{..}), v) = do
  void $ P.monitor scope
  notifySubscribers keyIdentity state (KeyRegistered scope)
  reply RegisteredOk $ ( (properties ^: Map.insert (scope, keyIdentity) v)
                       $ state )

handleLookupProperty :: forall k v. (Keyable k, Serializable v)
                     => State k v
                     -> LookupPropReq k
                     -> Process (ProcessReply LookupPropReply (State k v))
handleLookupProperty state (PropReq Key{..}) = do
  let entry = Map.lookup (fromJust keyScope, keyIdentity) (state ^. properties)
  case entry of
    Nothing -> reply PropNotFound state
    Just p  -> reply (PropFound (wrapMessage p)) state

handleUnregisterName :: forall k v. (Keyable k, Serializable v)
                     => State k v
                     -> UnregisterKeyReq k
                     -> Process (ProcessReply UnregisterKeyReply (State k v))
handleUnregisterName state (UnregisterKeyReq Key{..}) = do
  let entry = Map.lookup keyIdentity (state ^. names)
  case entry of
    Nothing  -> reply UnregisterKeyNotFound state
    Just pid ->
      case (pid /= (fromJust keyScope)) of
        True  -> reply UnregisterInvalidKey state
        False -> do
          notifySubscribers keyIdentity state KeyUnregistered
          let state' = ( (names ^: Map.delete keyIdentity)
                       . (monitors ^: MultiMap.filterWithKey (\k' _ -> k' /= keyIdentity))
                       $ state)
          reply UnregisterOk $ state'

handleGiveAwayName :: forall k v. (Keyable k, Serializable v)
                   => State k v
                   -> GiveAwayName k
                   -> Process (ProcessAction (State k v))
handleGiveAwayName state (GiveAwayName newPid Key{..}) = do
  maybe (continue state) giveAway $ Map.lookup keyIdentity (state ^. names)
  where
    giveAway pid = do
      let scope = fromJust keyScope
      case (pid == scope) of
        False -> continue state
        True -> do
          let state' = ((names ^: Map.insert keyIdentity newPid) $ state)
          notifySubscribers keyIdentity state (KeyOwnerChanged pid newPid)
          continue state'

handleMonitorReq :: forall k v. (Keyable k, Serializable v)
                 => State k v
                 -> CallRef RegKeyMonitorRef
                 -> MonitorReq k
                 -> Process (ProcessReply RegKeyMonitorRef (State k v))
handleMonitorReq state cRef (MonitorReq Key{..} mask') = do
  let mRefId = (state ^. monitorIdCount) + 1
  Just caller <- resolve cRef
  let mRef  = RegKeyMonitorRef (caller, mRefId)
  let kmRef = KMRef mRef mask'
  let refs = state ^. listeningPids
  refs' <- ensureMonitored caller refs
  fireEventForPreRegisteredKey state keyIdentity keyScope kmRef
  reply mRef $ ( (monitors ^: MultiMap.insert keyIdentity kmRef)
               . (listeningPids ^= refs')
               . (monitorIdCount ^= mRefId)
               $ state
               )
  where
    fireEventForPreRegisteredKey st kId kScope KMRef{..} = do
      let evMask = maybe [] id mask
      case (keyType, elem OnKeyRegistered evMask) of
        (KeyTypeAlias, True) -> do
          let found = Map.lookup kId (st ^. names)
          fireEvent found kId ref
        (KeyTypeProperty, _) -> do
          self <- getSelfPid
          let scope = maybe self id kScope
          let found = Map.lookup (scope, kId) (st ^. properties)
          case found of
            Nothing -> return () -- TODO: logging or some such!?
            Just _  -> fireEvent (Just scope) kId ref
        _ -> return ()

    fireEvent fnd kId' ref' = do
      case fnd of
        Nothing -> return ()
        Just p  -> do
          us <- getSelfPid
          sendTo ref' $ (RegistryKeyMonitorNotification kId'
                         ref'
                         (KeyRegistered p)
                         us)

handleUnmonitorReq :: forall k v. (Keyable k, Serializable v)
                 => State k v
                 -> CallRef ()
                 -> UnmonitorReq
                 -> Process (ProcessReply () (State k v))
handleUnmonitorReq state cRef (UnmonitorReq ref') = do
  let pid = fst $ unRef ref'
  reply () $ ( (monitors ^: MultiMap.filter ((/= ref') . ref))
             . (listeningPids ^: Set.delete pid)
             $ state
             )

handleRegNamesLookup :: forall k v. (Keyable k, Serializable v)
                     => RegNamesReq
                     -> RestrictedProcess (State k v) (Result [k])
handleRegNamesLookup (RegNamesReq p) = do
  state <- getState
  Restricted.reply $ Map.foldlWithKey' (acc p) [] (state ^. names)
  where
    acc pid ns n pid'
      | pid == pid' = (n:ns)
      | otherwise   = ns

handleMonitorSignal :: forall k v. (Keyable k, Serializable v)
                    => State k v
                    -> ProcessMonitorNotification
                    -> Process (ProcessAction (State k v))
handleMonitorSignal state@State{..} (ProcessMonitorNotification _ pid diedReason) =
  do let state' = removeActiveSubscriptions pid state
     (deadNames, deadProps) <- notifyListeners state' pid diedReason
     continue $ ( (names ^= Map.difference _names deadNames)
                . (properties ^= Map.difference _properties deadProps)
                $ state)
  where
    removeActiveSubscriptions p s =
      let subscriptions = (state ^. listeningPids) in
      case (Set.member p subscriptions) of
        False -> s
        True  -> ( (listeningPids ^: Set.delete p)
                   -- delete any monitors this (now dead) process held
                 . (monitors ^: MultiMap.filter ((/= p) . fst . unRef . ref))
                 $ s)

    notifyListeners :: State k v
                    -> ProcessId
                    -> DiedReason
                    -> Process (HashMap k ProcessId, HashMap (ProcessId, k) v)
    notifyListeners st pid' dr = do
      let diedNames = Map.filter (== pid') (st ^. names)
      let diedProps = Map.filterWithKey (\(p, _) _ -> p == pid')
                                        (st ^. properties)
      let nameSubs  = MultiMap.filterWithKey (\k _ -> Map.member k diedNames)
                                             (st ^. monitors)
      let propSubs  = MultiMap.filterWithKey (\k _ -> Map.member (pid', k) diedProps)
                                             (st ^. monitors)
      forM_ (MultiMap.toList nameSubs) $ \(kIdent, KMRef{..}) -> do
        let kEvDied = KeyOwnerDied { diedReason = dr }
        let mRef    = RegistryKeyMonitorNotification kIdent ref
        us <- getSelfPid
        case mask of
          Nothing    -> sendTo ref (mRef kEvDied us)
          Just mask' -> do
            case (elem OnKeyOwnershipChange mask') of
              True  -> sendTo ref (mRef kEvDied us)
              False -> do
                if (elem OnKeyUnregistered mask')
                  then sendTo ref (mRef KeyUnregistered us)
                  else return ()
      forM_ (MultiMap.toList propSubs) (notifyPropSubscribers dr)
      return (diedNames, diedProps)

    notifyPropSubscribers dr' (kIdent, KMRef{..}) = do
      let died  = maybe False (elem OnKeyOwnershipChange) mask
      let event = case died of
                    True  -> KeyOwnerDied { diedReason = dr' }
                    False -> KeyUnregistered
      getSelfPid >>= sendTo ref . RegistryKeyMonitorNotification kIdent ref event

ensureMonitored :: ProcessId -> HashSet ProcessId -> Process (HashSet ProcessId)
ensureMonitored pid refs = do
  case (Set.member pid refs) of
    True  -> return refs
    False -> P.monitor pid >> return (Set.insert pid refs)

notifySubscribers :: forall k v. (Keyable k, Serializable v)
                  => k
                  -> State k v
                  -> KeyUpdateEvent
                  -> Process ()
notifySubscribers k st ev = do
  let subscribers = MultiMap.filterWithKey (\k' _ -> k' == k) (st ^. monitors)
  forM_ (MultiMap.toList subscribers) $ \(_, KMRef{..}) -> do
    if (maybe True (elem (maskFor ev)) mask)
      then getSelfPid >>= sendTo ref . RegistryKeyMonitorNotification k ref ev
      else {- (liftIO $ putStrLn "no mask") >> -} return ()

--------------------------------------------------------------------------------
-- Utilities / Accessors                                                      --
--------------------------------------------------------------------------------

maskFor :: KeyUpdateEvent -> KeyUpdateEventMask
maskFor (KeyRegistered _)     = OnKeyRegistered
maskFor KeyUnregistered       = OnKeyUnregistered
maskFor (KeyOwnerDied   _)    = OnKeyOwnershipChange
maskFor (KeyOwnerChanged _ _) = OnKeyOwnershipChange
maskFor KeyLeaseExpired       = OnKeyLeaseExpiry

findName :: forall k v. (Keyable k, Serializable v)
         => Key k
         -> State k v
         -> Maybe ProcessId
findName Key{..} state = Map.lookup keyIdentity (state ^. names)

names :: forall k v. Accessor (State k v) (HashMap k ProcessId)
names = accessor _names (\n' st -> st { _names = n' })

properties :: forall k v. Accessor (State k v) (HashMap (ProcessId, k) v)
properties = accessor _properties (\ps st -> st { _properties = ps })

monitors :: forall k v. Accessor (State k v) (MultiMap k KMRef)
monitors = accessor _monitors (\ms st -> st { _monitors = ms })

registeredPids :: forall k v. Accessor (State k v) (HashSet ProcessId)
registeredPids = accessor _registeredPids (\mp st -> st { _registeredPids = mp })

listeningPids :: forall k v. Accessor (State k v) (HashSet ProcessId)
listeningPids = accessor _listeningPids (\lp st -> st { _listeningPids = lp })

monitorIdCount :: forall k v. Accessor (State k v) Integer
monitorIdCount = accessor _monitorIdCount (\i st -> st { _monitorIdCount = i })

