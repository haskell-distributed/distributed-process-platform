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

import Control.Distributed.Process hiding (call, monitor, mask)
import qualified Control.Distributed.Process as P (monitor)
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
  , putState
  , modifyState
  )
import qualified Control.Distributed.Process.Platform.ManagedProcess.Server.Restricted as Restricted
  ( handleCallIf
  , handleCall
  , handleCast
  , reply
  , continue
  )
-- import Control.Distributed.Process.Platform.ManagedProcess.Server.Unsafe
-- import Control.Distributed.Process.Platform.ManagedProcess.Server
import Control.Distributed.Process.Platform.Time
import Control.Exception (SomeException, Exception, throwIO)
import Control.Monad (forM_, void)
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

class (Eq a, Hashable a, Serializable a) => Keyable a
instance (Eq a, Hashable a, Serializable a) => Keyable a

data KeyView = K !KeyType !(Maybe ProcessId)
  deriving (Typeable, Eq)

view :: (Serializable a) => Key a -> KeyView
view (Key _ t s) = K t s

data KeyUpdateEventMask =
    OnKeyUnregistered
  | OnKeyOwnershipChange
  | OnKeyValueChange
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

data KeyUpdateEvent k =
    KeyNotRecognised
  | KeyUnregistered
    {
      key :: !k
    }
  | KeyLeaseExpired
    {
      key :: !k
    }
  | KeyOwnerDied
    {
      key        :: !k
    , diedReason :: !DiedReason
    }
  | KeyOwnerChanged
    {
      previousOwner :: !ProcessId
    , newOwner      :: !ProcessId
    , key           :: !k
    }
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (KeyUpdateEvent k)
deriving instance (Eq k) => Eq (KeyUpdateEvent k)
deriving instance (Show k) => Show (KeyUpdateEvent k)

data RegistryKeyMonitorNotification k =
  RegistryKeyMonitorNotification !k !RegKeyMonitorRef !(KeyUpdateEvent k)
  deriving (Typeable, Generic)
instance (Keyable k) => Binary (RegistryKeyMonitorNotification k) where
deriving instance (Eq k) => Eq (RegistryKeyMonitorNotification k)
deriving instance (Show k) => Show (RegistryKeyMonitorNotification k)

data KMRef = KMRef { ref  :: !RegKeyMonitorRef
                   , mask :: !(Maybe [KeyUpdateEventMask])
                     -- use Nothing to monitor every event
                   }

data State k v =
  State
  {
    _names          :: !(HashMap k ProcessId)
  , _properties     :: !(HashMap (ProcessId, k) v)
  , _monitors       :: !(HashMap k KMRef)
  , _monitoredPids  :: !(HashSet ProcessId)
  , _monitorIdCount :: !Integer
  }
  deriving (Typeable, Generic)

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

data Registry k v = LocalRegistry | RemoteRegistry

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
initIt _ () = return $ InitOk initState Infinity
  where
    initState = State { _names          = Map.empty
                      , _properties     = Map.empty
                      , _monitors       = Map.empty
                      , _monitoredPids  = Set.empty
                      , _monitorIdCount = (1 :: Integer)
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
       , Restricted.handleCallIf
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
      let refs = state ^. monitoredPids
      refs' <- ensureMonitored pid refs
      reply RegisteredOk $ ( (names ^: Map.insert keyIdentity pid)
                           . (monitoredPids ^= refs')
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
                     => UnregisterKeyReq k
                     -> RestrictedProcess (State k v) (Result UnregisterKeyReply)
handleUnregisterName (UnregisterKeyReq Key{..}) = do
  state <- getState
  let entry = Map.lookup keyIdentity (state ^. names)
  case entry of
    Nothing  -> Restricted.reply UnregisterKeyNotFound
    Just pid ->
      case (pid /= (fromJust keyScope)) of
        True  -> Restricted.reply UnregisterInvalidKey
        False -> do
          putState $ ((names ^: Map.delete keyIdentity) $ state)
          Restricted.reply UnregisterOk

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
  reply mRef $ ( (monitors ^: Map.insert keyIdentity kmRef)
               . (monitorIdCount ^= mRefId)
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
handleMonitorSignal state (ProcessMonitorNotification _ pid reason) = do
  (regNames, diedNames) <- notifyListeners state pid reason
  continue $ ((names ^= Map.difference regNames diedNames) $ state)

notifyListeners :: forall k v. (Keyable k, Serializable v)
                => State k v
                -> ProcessId
                -> DiedReason
                -> Process (HashMap k ProcessId, HashMap k ProcessId)
notifyListeners state pid dr = do
  let regNames    = state ^. names
  let monitored   = state ^. monitors
  let diedNames   = Map.filter (== pid) regNames
  let subscribers = Map.filterWithKey (\k _ -> Map.member k diedNames) monitored
  parent <- getSelfPid
  void $ spawnLocal $ do
    forM_ (Map.toList subscribers) $ \(kIdent, KMRef{..}) -> do
      case mask of
        Nothing -> do
          let kEv  = KeyOwnerDied { key = kIdent, diedReason = dr }
          let mRef = RegistryKeyMonitorNotification kIdent ref kEv
          sendTo ref mRef
        Just _ ->
          kill parent "NOT IMPLEMENTED YET"
  return (regNames, diedNames)

--------------------------------------------------------------------------------
-- Internal/State Handling API                                                --
--------------------------------------------------------------------------------

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

monitoredPids :: forall k v. Accessor (State k v) (HashSet ProcessId)
monitoredPids = accessor _monitoredPids (\mp st -> st { _monitoredPids = mp })

monitorIdCount :: forall k v. Accessor (State k v) Integer
monitorIdCount = accessor _monitorIdCount (\i st -> st { _monitorIdCount = i })

