{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE StandaloneDeriving         #-}

-- | Types used throughout the ManagedProcess framework
module Control.Distributed.Process.Platform.ManagedProcess.Internal.Types
  ( -- * Exported data types
    InitResult(..)
  , Condition(..)
  , ProcessAction(..)
  , ProcessReply(..)
  , CallHandler
  , CastHandler
  , DeferredCallHandler
  , StatelessCallHandler
  , InfoHandler
  , ChannelHandler
  , StatelessChannelHandler
  , InitHandler
  , ShutdownHandler
  , TimeoutHandler
  , UnhandledMessagePolicy(..)
  , ProcessDefinition(..)
  , Priority(..)
  , DispatchPriority(..)
  , PrioritisedProcessDefinition(..)
  , Dispatcher(..)
  , DeferredDispatcher(..)
  , ExitSignalDispatcher(..)
  , MessageMatcher(..)
  , DynMessageHandler(..)
  , Message(..)
  , CallResponse(..)
  , CallId
  , CallRef(..)
  , makeRef
  ) where

import Control.Distributed.Process hiding (Message)
import qualified Control.Distributed.Process as P (Message)
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Platform.Internal.Primitives
  ( Addressable(..)
  )
import Control.Distributed.Process.Platform.Internal.Types
  ( Recipient(..)
  , ExitReason(..)
  )
import Control.Distributed.Process.Platform.Time

import Data.Binary hiding (decode)
import Data.Typeable (Typeable)

import Prelude hiding (init)

import GHC.Generics

--------------------------------------------------------------------------------
-- API                                                                        --
--------------------------------------------------------------------------------

type CallId = MonitorRef

newtype CallRef a = CallRef { unCaller :: (Recipient, CallId) }
  deriving (Eq, Show, Typeable, Generic)
instance Serializable a => Binary (CallRef a) where

makeRef :: forall a . (Serializable a) => Recipient -> CallId -> CallRef a
makeRef r c = CallRef (r, c)

instance Addressable (CallRef a) where
  resolve (CallRef (r, _))            = resolve r
  sendTo  (CallRef (client, tag)) msg = sendTo client (CallResponse msg tag)

data Message a b =
    CastMessage a
  | CallMessage a (CallRef b)
  | ChanMessage a (SendPort b)
  deriving (Typeable, Generic)

instance (Serializable a, Serializable b) => Binary (Message a b) where
deriving instance (Eq a, Eq b) => Eq (Message a b)
deriving instance (Show a, Show b) => Show (Message a b)

data CallResponse a = CallResponse a CallId
  deriving (Typeable, Generic)

instance Serializable a => Binary (CallResponse a)
deriving instance Eq a => Eq (CallResponse a)
deriving instance Show a => Show (CallResponse a)

-- | Return type for and 'InitHandler' expression.
data InitResult s =
    InitOk s Delay {-
        ^ a successful initialisation, initial state and timeout -}
  | InitStop String {-
        ^ failed initialisation and the reason, this will result in an error -}
  | InitIgnore {-
        ^ the process has decided not to continue starting - this is not an error -}
  deriving (Typeable)

-- | The action taken by a process after a handler has run and its updated state.
-- See 'continue'
--     'timeoutAfter'
--     'hibernate'
--     'stop'
--
data ProcessAction s =
    ProcessContinue  s              -- ^ continue with (possibly new) state
  | ProcessTimeout   TimeInterval s -- ^ timeout if no messages are received
  | ProcessHibernate TimeInterval s -- ^ hibernate for /delay/
  | ProcessStop      ExitReason     -- ^ stop the process, giving @ExitReason@

-- | Returned from handlers for the synchronous 'call' protocol, encapsulates
-- the reply data /and/ the action to take after sending the reply. A handler
-- can return @NoReply@ if they wish to ignore the call.
data ProcessReply r s =
    ProcessReply r (ProcessAction s)
  | NoReply (ProcessAction s)

-- | Wraps a predicate that is used to determine whether or not a handler
-- is valid based on some combination of the current process state, the
-- type and/or value of the input message or both.
data Condition s m =
    Condition (s -> m -> Bool)  -- ^ predicated on the process state /and/ the message
  | State     (s -> Bool)       -- ^ predicated on the process state only
  | Input     (m -> Bool)       -- ^ predicated on the input message only


-- | An expression used to handle a /call/ message.
type CallHandler s a b = s -> a -> Process (ProcessReply b s)

-- | An expression used to handle a /call/ message where the reply is deferred
-- via the 'CallRef'.
type DeferredCallHandler s a b = s -> CallRef b -> a -> Process (ProcessReply b s)

-- | An expression used to handle a /call/ message in a stateless process.
type StatelessCallHandler a b = a -> CallRef b -> Process (ProcessReply b ())

-- | An expression used to handle a /cast/ message.
type CastHandler s a = s -> a -> Process (ProcessAction s)

-- | An expression used to handle an /info/ message.
type InfoHandler s a = s -> a -> Process (ProcessAction s)

-- | An expression used to handle a /channel/ message.
type ChannelHandler s a b = s -> SendPort b -> a -> Process (ProcessAction s)

-- | An expression used to handle a /channel/ message in a stateless process.
type StatelessChannelHandler a b = SendPort b -> a -> Process (ProcessAction ())

-- | An expression used to initialise a process with its state.
type InitHandler a s = a -> Process (InitResult s)

-- | An expression used to handle process termination.
type ShutdownHandler s = s -> ExitReason -> Process ()

-- | An expression used to handle process timeouts.
type TimeoutHandler s = s -> Delay -> Process (ProcessAction s)

-- dispatching to implementation callbacks

-- | Provides dispatch from cast and call messages to a typed handler.
data Dispatcher s =
    forall a b . (Serializable a, Serializable b) =>
    Dispatch
    {
      dispatch :: s -> Message a b -> Process (ProcessAction s)
    }
  | forall a b . (Serializable a, Serializable b) =>
    DispatchIf
    {
      dispatch   :: s -> Message a b -> Process (ProcessAction s)
    , dispatchIf :: s -> Message a b -> Bool
    }

-- | Provides dispatch for any input, returns 'Nothing' for unhandled messages.
data DeferredDispatcher s =
  DeferredDispatcher
  {
    dispatchInfo :: s
                 -> P.Message
                 -> Process (Maybe (ProcessAction s))
  }

-- | Provides dispatch for any exit signal - returns 'Nothing' for unhandled exceptions
data ExitSignalDispatcher s =
  ExitSignalDispatcher
  {
    dispatchExit :: s
                 -> ProcessId
                 -> P.Message
                 -> Process (Maybe (ProcessAction s))
  }

class MessageMatcher d where
  matchDispatch :: UnhandledMessagePolicy -> s -> d s -> Match (ProcessAction s)

instance MessageMatcher Dispatcher where
  matchDispatch _ s (Dispatch   d)      = match (d s)
  matchDispatch _ s (DispatchIf d cond) = matchIf (cond s) (d s)

class DynMessageHandler d where
  dynHandleMessage :: UnhandledMessagePolicy
                   -> s
                   -> d s
                   -> P.Message
                   -> Process (Maybe (ProcessAction s))

instance DynMessageHandler Dispatcher where
  dynHandleMessage _ s (Dispatch   d)   msg = handleMessage   msg (d s)
  dynHandleMessage _ s (DispatchIf d c) msg = handleMessageIf msg (c s) (d s)

instance DynMessageHandler DeferredDispatcher where
  dynHandleMessage _ s (DeferredDispatcher d) = d s

newtype Priority a = Priority { getPrio :: Int }

data DispatchPriority s =
    PrioritiseCall
    {
      prioritise :: s -> P.Message -> Process (Maybe (Int, P.Message))
    }
  | PrioritiseCast
    {
      prioritise :: s -> P.Message -> Process (Maybe (Int, P.Message))
    }
  | PrioritiseInfo
    {
      prioritise :: s -> P.Message -> Process (Maybe (Int, P.Message))
    }

data PrioritisedProcessDefinition s =
  PrioritisedProcessDefinition
  {
    processDef :: ProcessDefinition s
  , priorities :: [DispatchPriority s]
  }

-- | Policy for handling unexpected messages, i.e., messages which are not
-- sent using the 'call' or 'cast' APIs, and which are not handled by any of the
-- 'handleInfo' handlers.
data UnhandledMessagePolicy =
    Terminate  -- ^ stop immediately, giving @ExitOther "UnhandledInput"@ as the reason
  | DeadLetter ProcessId -- ^ forward the message to the given recipient
  | Drop                 -- ^ dequeue and then drop/ignore the message

-- | Stores the functions that determine runtime behaviour in response to
-- incoming messages and a policy for responding to unhandled messages.
data ProcessDefinition s = ProcessDefinition {
    apiHandlers  :: [Dispatcher s]     -- ^ functions that handle call/cast messages
  , infoHandlers :: [DeferredDispatcher s] -- ^ functions that handle non call/cast messages
  , exitHandlers :: [ExitSignalDispatcher s] -- ^ functions that handle exit signals
  , timeoutHandler :: TimeoutHandler s   -- ^ a function that handles timeouts
  , shutdownHandler :: ShutdownHandler s -- ^ a function that is run just before the process exits
  , unhandledMessagePolicy :: UnhandledMessagePolicy -- ^ how to deal with unhandled messages
  }

