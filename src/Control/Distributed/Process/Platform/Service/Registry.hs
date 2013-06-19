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
-- * Associate (unique) keys with a process /or/ (unique keys per-process) values
-- * Use any 'Keyable' algebraic data type (beside 'String') as a key/name
-- * Query for process with matching keys / values / properties
-- * Atomically /give away/ properties or names
-- * Forceibly re-allocate names from a third party
--
-- [Subscribing To Registry Events]
--
-- It is possible to monitor a registry for changes and be informed whenever
-- changes take place. All subscriptions are /key based/, which means that
-- you can subscribe to name or property changes for any process, though any
-- property changes matching the key you've subscribed to will trigger a
-- notification (i.e., regardless of the process to which the property belongs).
--
-- The different events are defined by the 'KeyUpdateEvent' type.
--
-- Processes subscribe to registry events using @monitorName@ or its counterpart
-- @monitorProperty@. If the operation succeeds, this will evaluate to an
-- opaque /reference/ which can be used in subsequently handling any received
-- notifications, which will be delivered to the subscriber's mailbox as
-- @RegistryKeyMonitorNotification keyIdentity opaqueRef event@, where @event@
-- has the type 'KeyUpdateEvent'.
--
-- Subscribers can filter the types of event they receive by using the lower
-- level @monitor@ function (defined in /this/ module - not the one defined
-- in distributed-process' Primitives) and passing a list of
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
  , RegisterKeyReply(..)
  , unregisterName
  , UnregisterKeyReply(..)
    -- * Queries / Lookups
  , lookupName
  , registeredNames
    -- * Monitoring / Waiting
  , monitor
  , monitorName
  , KeyUpdateEventMask(..)
  , KeyUpdateEvent(..)
  , RegKeyMonitorRef
  , RegistryKeyMonitorNotification(RegistryKeyMonitorNotification)
  ) where

import Control.Distributed.Process hiding (call, monitor, unmonitor, mask)
import qualified Control.Distributed.Process as P (monitor, unmonitor)
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Platform.Internal.Primitives hiding (monitor)
import Control.Distributed.Process.Platform.ManagedProcess
  ( call
  , handleCall
  , handleInfo
  , reply
  , continue
  , stop
  , stopWith
  , input
  , defaultProcess
  , prioritised
  , InitHandler
  , InitResult(..)
  , ProcessAction
  , ProcessReply
  , ProcessDefinition(..)
  , PrioritisedProcessDefinition(..)
  , Priority(..)
  , DispatchPriority
  , UnhandledMessagePolicy(Drop)
  , CallRef
  )
import qualified Control.Distributed.Process.Platform.ManagedProcess as MP
  ( pserve
  )
import Control.Distributed.Process.Platform.ManagedProcess.Server
  ( handleCast
  , handleCall
  , handleCallIf
  , handleCallFrom
  , reply
  , continue
  , stop
  )
import Control.Distributed.Process.Platform.ManagedProcess.Server.Priority
  ( prioritiseCast_
  , prioritiseCall_
  , prioritiseInfo_
  , setPriority
  )
import Control.Distributed.Process.Platform.ManagedProcess.Server.Restricted
  ( RestrictedProcess
  , Result
  , RestrictedAction
  , getState
  )
import qualified Control.Distributed.Process.Platform.ManagedProcess.Server.Restricted as Restricted
  ( handleCall
  , reply
  )
-- import Control.Distributed.Process.Platform.ManagedProcess.Server.Unsafe
-- import Control.Distributed.Process.Platform.ManagedProcess.Server
import Control.Distributed.Process.Platform.Time
import Control.Exception (SomeException, Exception, throwIO)
import Control.Monad (forM_)
import Data.Accessor
  ( Accessor
  , accessor
  , (^:)
  , (.>)
  , (^=)
  , (^.)
  )
import Data.Binary
import Data.Maybe (fromJust, isJust)
import Data.Hashable
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as Map
import Data.HashSet (HashSet)
import qualified Data.HashSet as Set
import Data.Typeable (Typeable)

import GHC.Generics

--------------------------------------------------------------------------------
-- Types                                                                      --
--------------------------------------------------------------------------------

data KeyType =
    KeyTypeAlias
  | KeyTypeProperty
  deriving (Typeable, Generic, Show, Eq)
instance Binary KeyType where
instance Hashable KeyType where

data Key a =
    Key
    { keyIdentity :: !a
    , keyType     :: !KeyType
    , keyScope    :: !(Maybe ProcessId)
    }
  deriving (Typeable, Generic, Show, Eq)
instance (Serializable a) => Binary (Key a) where
instance (Hashable a) => Hashable (Key a) where

class (Show a, Eq a, Hashable a, Serializable a) => Keyable a
instance (Show a, Eq a, Hashable a, Serializable a) => Keyable a

data KeyUpdateEventMask =
    OnKeyRegistered
  | OnKeyUnregistered
  | OnKeyOwnershipChange
  | OnKeyLeaseExpiry
  deriving (Typeable, Generic, Eq, Show)
instance Binary KeyUpdateEventMask where

newtype RegKeyMonitorRef =
  RegKeyMonitorRef { unRef :: (ProcessId, Integer) }
  deriving (Typeable, Generic, Eq, Show)
instance Binary RegKeyMonitorRef where
instance Hashable RegKeyMonitorRef where

instance Addressable RegKeyMonitorRef where
  resolve = return . Just . fst . unRef

data KeyUpdateEvent =
    KeyRegistered
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

data RegistryKeyMonitorNotification k =
  RegistryKeyMonitorNotification !k !RegKeyMonitorRef !KeyUpdateEvent
  deriving (Typeable, Generic)
instance (Keyable k) => Binary (RegistryKeyMonitorNotification k) where
deriving instance (Keyable k) => Eq (RegistryKeyMonitorNotification k)
deriving instance (Keyable k) => Show (RegistryKeyMonitorNotification k)

data RegisterKeyReq k = RegisterKeyReq !(Key k)
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (RegisterKeyReq k) where

data RegisterKeyReply = RegisteredOk | AlreadyRegistered
  deriving (Typeable, Generic, Eq, Show)
instance Binary RegisterKeyReply where

data LookupKeyReq k = LookupKeyReq !(Key k)
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (LookupKeyReq k) where

data RegNamesReq = RegNamesReq !ProcessId
  deriving (Typeable, Generic)
instance Binary RegNamesReq where

data UnregisterKeyReq k = UnregisterKeyReq !(Key k)
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (UnregisterKeyReq k) where

data UnregisterKeyReply =
    UnregisterOk
  | UnregisterInvalidKey
  | UnregisterKeyNotFound
  deriving (Typeable, Generic, Eq, Show)
instance Binary UnregisterKeyReply where

data MonitorReq k = MonitorReq !(Key k) !(Maybe [KeyUpdateEventMask])
  deriving (Typeable, Generic)
instance (Keyable k) => Binary (MonitorReq k) where

data Registry k v = LocalRegistry

data KMRef = KMRef { ref  :: !RegKeyMonitorRef
                   , mask :: !(Maybe [KeyUpdateEventMask])
                     -- use Nothing to monitor every event
                   }
  deriving (Show)

data State k v =
  State
  {
    _names          :: !(HashMap k ProcessId)
  , _properties     :: !(HashMap (ProcessId, k) v)
  , _monitors       :: !(HashMap k KMRef)
  , _registeredPids :: !(HashSet ProcessId)
  , _listeningPids  :: !(HashSet ProcessId)
  , _monitorIdCount :: !Integer
  , _registryType   :: !(Registry k v)
  }
  deriving (Typeable, Generic)

--------------------------------------------------------------------------------
-- Starting / Running A Registry                                              --
--------------------------------------------------------------------------------

start :: forall k v. (Keyable k, Serializable v)
      => Registry k v
      -> Process ProcessId
start reg = spawnLocal $ run reg

-- | Run the supplied children using the provided restart strategy.
--
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
                      , _monitors       = Map.empty
                      , _registeredPids = Set.empty
                      , _listeningPids  = Set.empty
                      , _monitorIdCount = (1 :: Integer)
                      , _registryType   = reg
                      } :: State k v

--------------------------------------------------------------------------------
-- Client Facing API                                                          --
--------------------------------------------------------------------------------

-- | Associate the current process with the given (unique) key.
addName :: (Addressable a, Keyable k) => a -> k -> Process RegisterKeyReply
addName s n = getSelfPid >>= registerName s n

-- | Associate the given (non-unique) property with the current process.
addProperty :: (Serializable a, Keyable k, Serializable v)
            => a -> Key k -> v -> Process ()
addProperty = undefined

-- | Register the item at the given address.
registerName :: (Addressable a, Keyable k)
             => a -> k -> ProcessId -> Process RegisterKeyReply
registerName s n p = call s $ RegisterKeyReq (Key n KeyTypeAlias $ Just p)

-- | Register an item at the given address and associate it with a value.
registerValue :: (Addressable a, Keyable k, Serializable v)
              => a -> k -> v -> Process ()
registerValue = undefined

unregisterName :: (Addressable a, Keyable k)
               => a
               -> k
               -> Process UnregisterKeyReply
unregisterName s n = do
  self <- getSelfPid
  call s $ UnregisterKeyReq (Key n KeyTypeAlias $ Just self)

lookupName :: (Addressable a, Keyable k) => a -> k -> Process (Maybe ProcessId)
lookupName s n = call s $ LookupKeyReq (Key n KeyTypeAlias Nothing)

registeredNames :: (Addressable a, Keyable k) => a -> ProcessId -> Process [k]
registeredNames s p = call s $ RegNamesReq p

monitorName :: (Addressable a, Keyable k)
            => a -> k -> Process RegKeyMonitorRef
monitorName svr name = do
  let key' = Key { keyIdentity = name
                 , keyScope    = Nothing
                 , keyType     = KeyTypeAlias
                 }
  monitor svr key' Nothing

monitor :: (Addressable a, Keyable k)
        => a
        -> Key k
        -> Maybe [KeyUpdateEventMask]
        -> Process RegKeyMonitorRef
monitor svr key' mask' = call svr $ MonitorReq key' mask'

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
              (input ((\(LookupKeyReq (Key{..} :: Key k)) ->
                        keyType == KeyTypeAlias)))
              (\state (LookupKeyReq key') -> reply (findName key' state) state)
       , handleCallIf
              (input ((\(UnregisterKeyReq (Key{..} :: Key k)) ->
                        keyType == KeyTypeAlias && (isJust keyScope))))
              handleUnregisterName
       , handleCallFrom handleMonitorReq
       , Restricted.handleCall handleRegNamesLookup
       ]
  , infoHandlers = [handleInfo handleMonitorSignal]
  } :: ProcessDefinition (State k v)

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
      notifySubscribers keyIdentity state KeyRegistered
      reply RegisteredOk $ ( (names ^: Map.insert keyIdentity pid)
                           . (registeredPids ^= refs')
                           $ state)
    Just pid ->
      if (pid == (fromJust keyScope))
         then reply RegisteredOk      state
         else reply AlreadyRegistered state
  where
    ensureMonitored pid refs = do
      case (Set.member pid refs) of
        True  -> return refs
        False -> P.monitor pid >> return (Set.insert pid refs)

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
                       . (monitors ^: Map.filterWithKey (\k' _ -> k' /= keyIdentity))
                       $ state)
          reply UnregisterOk $ state'

handleMonitorReq :: forall k v. (Keyable k, Serializable v)
                 => State k v
                 -> CallRef RegKeyMonitorRef
                 -> MonitorReq k
                 -> Process (ProcessReply RegKeyMonitorRef (State k v))
handleMonitorReq state cRef (MonitorReq Key{..} mask) = do
  let mRefId = (state ^. monitorIdCount) + 1
  Just caller <- resolve cRef
  -- make a KMRef for the process...
  let mRef  = RegKeyMonitorRef (caller, mRefId)
  let kmRef = KMRef mRef mask
  let refs = state ^. listeningPids
  refs' <- ensureMonitored caller refs
  reply mRef $ ( (monitors ^: Map.insert keyIdentity kmRef)
               . (listeningPids ^= refs')
               . (monitorIdCount ^= mRefId)
               $ state
               )
  where
    ensureMonitored pid refs = do
      case (Set.member pid refs) of
        True  -> return refs
        False -> P.monitor pid >> return (Set.insert pid refs)

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
handleMonitorSignal state@State{..} (ProcessMonitorNotification _ pid reason) =
  do let state' = removeActiveSubscriptions pid state
     (deadNames, deadProps) <- notifyListeners state' pid reason
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
                 . (monitors ^: Map.filter ((/= p) . fst . unRef . ref))
                 $ s)

    notifyListeners :: State k v
                    -> ProcessId
                    -> DiedReason
                    -> Process (HashMap k ProcessId, HashMap (ProcessId, k) v)
    notifyListeners st pid' dr = do
      let diedNames = Map.filter (== pid') (st ^. names)
      let diedProps = Map.filterWithKey (\(p, _) _ -> p == pid')
                                        (st ^. properties)
      let nameSubs  = Map.filterWithKey (\k _ -> Map.member k diedNames)
                                        (st ^. monitors)
      let propSubs  = Map.filterWithKey (\k _ -> Map.member (pid', k) diedProps)
                                        (st ^. monitors)
      forM_ (Map.toList nameSubs) $ \(kIdent, KMRef{..}) -> do
        let kEvDied = KeyOwnerDied { diedReason = dr }
        let mRef    = RegistryKeyMonitorNotification kIdent ref
        case mask of
          Nothing    -> sendTo ref (mRef kEvDied)
          Just mask' -> do
            case (elem OnKeyOwnershipChange mask') of
              True  -> sendTo ref (mRef kEvDied)
              False -> do
                if (elem OnKeyUnregistered mask')
                  then sendTo ref (mRef KeyUnregistered)
                  else return ()
      forM_ (Map.toList propSubs) (notifyPropSubscribers dr)
      return (diedNames, diedProps)

    notifyPropSubscribers dr' (kIdent, KMRef{..}) = do
      let died  = maybe False (elem OnKeyOwnershipChange) mask
      let event = case died of
                    True  -> KeyOwnerDied { diedReason = dr' }
                    False -> KeyUnregistered
      sendTo ref $ RegistryKeyMonitorNotification kIdent ref event

notifySubscribers :: forall k v. (Keyable k, Serializable v)
                  => k
                  -> State k v
                  -> KeyUpdateEvent
                  -> Process ()
notifySubscribers k st ev = do
  let subscribers = Map.filterWithKey (\k' _ -> k' == k) (st ^. monitors)
  forM_ (Map.toList subscribers) $ \(_, KMRef{..}) -> do
    if (maybe True (elem (maskFor ev)) mask)
      then sendTo ref $ RegistryKeyMonitorNotification k ref ev
      else return ()

--------------------------------------------------------------------------------
-- Utilities / Accessors                                                      --
--------------------------------------------------------------------------------

maskFor :: KeyUpdateEvent -> KeyUpdateEventMask
maskFor KeyRegistered         = OnKeyRegistered
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

monitors :: forall k v. Accessor (State k v) (HashMap k KMRef)
monitors = accessor _monitors (\ms st -> st { _monitors = ms })

registeredPids :: forall k v. Accessor (State k v) (HashSet ProcessId)
registeredPids = accessor _registeredPids (\mp st -> st { _registeredPids = mp })

listeningPids :: forall k v. Accessor (State k v) (HashSet ProcessId)
listeningPids = accessor _listeningPids (\lp st -> st { _listeningPids = lp })

monitorIdCount :: forall k v. Accessor (State k v) Integer
monitorIdCount = accessor _monitorIdCount (\i st -> st { _monitorIdCount = i })

