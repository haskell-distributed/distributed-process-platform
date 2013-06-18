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
module Control.Distributed.Process.Platform.Service.Registry where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Platform.Internal.Primitives hiding (monitor)
import Control.Distributed.Process.Platform.Internal.Types
  ( ExitReason(..)
  )
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
  )
import qualified Control.Distributed.Process.Platform.ManagedProcess as MP
  ( serve
  , call
  )
import Control.Distributed.Process.Platform.ManagedProcess.Server
  ( handleCast
  , handleCall
  , handleCallIf
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

import Control.Monad.Error

import Data.Accessor
  ( Accessor
  , accessor
  , (^:)
  , (.>)
  , (^=)
  , (^.)
  )
import Data.Binary
import Data.Foldable (find, foldlM, toList)
import Data.List (foldl')
import Data.Maybe (fromJust, isJust)
import Data.Hashable
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as Map
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

data KeyUpdateEvent k =
    KeyNotRecognised
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
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (KeyUpdateEvent k)
deriving instance (Eq k) => Eq (KeyUpdateEvent k)
deriving instance (Show k) => Show (KeyUpdateEvent k)

data RegistryKeyMonitorNotification k =
  RegistryKeyMonitorNotification !k !RegKeyMonitorRef !(KeyUpdateEvent k)
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (RegistryKeyMonitorNotification k) where
deriving instance (Eq k) => Eq (RegistryKeyMonitorNotification k)
deriving instance (Show k) => Show (RegistryKeyMonitorNotification k)

data KMRef = KMRef { ref  :: !RegKeyMonitorRef
                   , mask :: !(Maybe [KeyUpdateEventMask])
                   }

data State k v =
  State
  {
    _names          :: !(HashMap k ProcessId)
  , _properties     :: !(HashMap (ProcessId, k) v)
  , _monitors       :: !(HashMap (ProcessId, k) KMRef)
  , _monitorIdCount :: !Integer
  }
  deriving (Typeable, Generic)

data RegisterKeyReq k = RegisterKeyReq { key :: !(Key k) }
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (RegisterKeyReq k) where

data RegisterKeyReply = RegisteredOk | AlreadyRegistered
  deriving (Typeable, Generic, Eq, Show)
instance Binary RegisterKeyReply where

data LookupKeyReq k = LookupKeyReq !(Key k)
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (LookupKeyReq k) where

data UnregisterKeyReq k = UnregisterKeyReq !(Key k)
  deriving (Typeable, Generic)
instance (Serializable k) => Binary (UnregisterKeyReq k) where

data UnregisterKeyReply =
    UnregisterOk
  | UnregisterInvalidKey
  | UnregisterKeyNotFound
  deriving (Typeable, Generic)
instance Binary UnregisterKeyReply where

data Registry k v = LocalRegistry | RemoteRegistry

--------------------------------------------------------------------------------
-- Starting / Running Registry                                                --
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
run reg = MP.serve () (initIt reg) processDefinition

initIt :: forall k v. (Keyable k, Serializable v)
       => Registry k v
       -> InitHandler () (State k v)
initIt _ () = return $ InitOk initState Infinity
  where
    initState = State { _names          = Map.empty
                      , _properties     = Map.empty
                      , _monitors       = Map.empty
                      , _monitorIdCount = (1 :: Integer)
                      } :: State k v

--------------------------------------------------------------------------------
-- Client Facing API                                                          --
--------------------------------------------------------------------------------

-- | Associate the current process with the given (unique) key.
addName :: (Addressable a, Keyable k)
        => a
        -> k
        -> Process RegisterKeyReply
addName s n = getSelfPid >>= registerName s n

-- | Associate the given (non-unique) property with the current process.
addProperty :: (Serializable a, Keyable k, Serializable v)
            => a
            -> Key k
            -> v
            -> Process ()
addProperty = undefined

-- | Register the item at the given address.
registerName :: (Addressable a, Keyable k)
             => a
             -> k
             -> ProcessId
             -> Process RegisterKeyReply
registerName s n p = call s $ RegisterKeyReq (Key n KeyTypeAlias $ Just p)

-- | Register an item at the given address and associate it with a value.
registerValue :: (Addressable a, Keyable k, Serializable v)
              => a
              -> k
              -> v
              -> Process ()
registerValue = undefined

unregisterName :: (Addressable a, Keyable k)
               => a
               -> k
               -> Process UnregisterKeyReply
unregisterName s n = do
  self <- getSelfPid
  call s $ UnregisterKeyReq (Key n KeyTypeAlias $ Just self)

lookupName :: (Addressable a, Keyable k)
           => a
           -> k
           -> Process (Maybe ProcessId)
lookupName s n = call s $ LookupKeyReq (Key n KeyTypeAlias Nothing)

--------------------------------------------------------------------------------
-- Server Process                                                             --
--------------------------------------------------------------------------------

processDefinition :: forall k v. (Keyable k, Serializable v)
                  => ProcessDefinition (State k v)
processDefinition =
  defaultProcess
  {
    apiHandlers =
       [
         Restricted.handleCallIf
              (input ((\(RegisterKeyReq (Key{..} :: Key k)) ->
                        keyType == KeyTypeAlias && (isJust keyScope))))
              handleRegisterName
       , handleCallIf
              (input ((\(LookupKeyReq (Key{..} :: Key k)) ->
                        keyType == KeyTypeAlias)))
              (\state (LookupKeyReq key) -> reply (findName key state) state)
       , Restricted.handleCallIf
              (input ((\(UnregisterKeyReq (Key{..} :: Key k)) ->
                        keyType == KeyTypeAlias && (isJust keyScope))))
              handleUnregisterName
       ]
  , infoHandlers = [{- handleMonitorSignal -}]
  } :: ProcessDefinition (State k v)

handleRegisterName :: forall k v. (Keyable k, Serializable v)
                   => RegisterKeyReq k
                   -> RestrictedProcess (State k v) (Result RegisterKeyReply)
handleRegisterName (RegisterKeyReq Key{..}) = do
  state <- getState
  let found = Map.lookup keyIdentity (state ^. names)
  case found of
    Nothing -> do
      putState $ ((names ^: Map.insert keyIdentity (fromJust keyScope)) $ state)
      Restricted.reply RegisteredOk
    Just pid ->
      if (pid == (fromJust keyScope))
         then Restricted.reply RegisteredOk
         else Restricted.reply AlreadyRegistered

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

findName :: forall k v. (Keyable k, Serializable v)
         => Key k
         -> State k v
         -> Maybe ProcessId
findName Key{..} state = Map.lookup keyIdentity (state ^. names)

names :: forall k v. Accessor (State k v) (HashMap k ProcessId)
names = accessor _names (\n' st -> st { _names = n' })

properties :: forall k v. Accessor (State k v) (HashMap (ProcessId, k) v)
properties = accessor _properties (\ps st -> st { _properties = ps })

monitors :: forall k v. Accessor (State k v) (HashMap (ProcessId, k) KMRef)
monitors = accessor _monitors (\ms st -> st { _monitors = ms })

monitorIdCount :: forall k v. Accessor (State k v) Integer
monitorIdCount = accessor _monitorIdCount (\i st -> st { _monitorIdCount = i })

